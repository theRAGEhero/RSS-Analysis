from __future__ import annotations

import argparse
import datetime as dt
import logging
import shutil
import sqlite3
import sys
from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine

from rss_transcriber import (
    DEFAULT_DB_PATH,
    DEFAULT_LOG_DIR,
    ensure_transcript_metadata_columns,
    setup_logging,
)


def configure_logging(verbose: bool, log_level: str, log_dir: Path, command: str) -> str:
    """Initialise structured logging aligned with the pipeline configuration."""
    level = "DEBUG" if verbose else (log_level or "INFO")
    run_label = f"db-{command}-{dt.datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
    setup_logging(log_dir, level, run_label=run_label)
    logging.getLogger(__name__).info("Logging initialised (run=%s, level=%s)", run_label, level.upper())
    return run_label


def export_database(db_path: Path, output_path: Optional[Path]) -> None:
    """Export the entire SQLite database to a SQL dump."""
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found at {db_path}")

    logging.debug("Opening database at %s for export", db_path)
    connection = sqlite3.connect(db_path)
    try:
        dump_iter = connection.iterdump()
        if output_path:
            output_path = output_path.expanduser().resolve()
            output_path.parent.mkdir(parents=True, exist_ok=True)
            logging.info("Writing SQL dump to %s", output_path)
            with output_path.open("w", encoding="utf-8", newline="\n") as handle:
                for line in dump_iter:
                    handle.write(f"{line}\n")
        else:
            logging.info("Writing SQL dump to stdout")
            stdout = sys.stdout
            for line in dump_iter:
                stdout.write(f"{line}\n")
            stdout.flush()
    finally:
        connection.close()


def _prepare_database_path(db_path: Path, force: bool, backup_path: Optional[Path]) -> Optional[Path]:
    """Ensure the target database path is ready for import and optionally back up existing data."""
    db_path = db_path.expanduser().resolve()
    backup_destination: Optional[Path] = None

    if db_path.exists():
        if not force:
            raise FileExistsError(
                f"Database already exists at {db_path}. Use --force to overwrite or choose a different path."
            )
        if backup_path:
            backup_destination = backup_path.expanduser().resolve()
        else:
            timestamp = dt.datetime.utcnow().strftime("%Y%m%d%H%M%S")
            backup_destination = db_path.with_suffix(db_path.suffix + f".{timestamp}.bak")

        if backup_destination.exists():
            raise FileExistsError(f"Backup destination already exists: {backup_destination}")

        backup_destination.parent.mkdir(parents=True, exist_ok=True)
        logging.info("Backing up existing database to %s", backup_destination)
        shutil.copy2(db_path, backup_destination)

        logging.debug("Removing existing database at %s", db_path)
        db_path.unlink()

    db_path.parent.mkdir(parents=True, exist_ok=True)
    return backup_destination


def import_database(
    db_path: Path,
    input_path: Optional[Path],
    force: bool,
    backup_path: Optional[Path],
    skip_upgrade: bool,
) -> Optional[Path]:
    """Import a SQL dump into the SQLite database."""
    db_path = db_path.expanduser().resolve()
    if input_path:
        input_path = input_path.expanduser().resolve()
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")
        logging.info("Reading SQL dump from %s", input_path)
        dump_text = input_path.read_text(encoding="utf-8")
    else:
        logging.info("Reading SQL dump from stdin")
        dump_text = sys.stdin.read()

    if not dump_text.strip():
        raise ValueError("No SQL statements provided for import.")

    backup_destination = _prepare_database_path(db_path, force=force, backup_path=backup_path)

    logging.debug("Creating new SQLite database at %s", db_path)
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript("PRAGMA foreign_keys=OFF;")
        connection.executescript(dump_text)
        connection.executescript("PRAGMA foreign_keys=ON;")
        connection.commit()
    except Exception:
        connection.rollback()
        logging.exception("Failed to import SQL dump; rolling back changes.")
        raise
    finally:
        connection.close()

    if not skip_upgrade:
        logging.debug("Ensuring transcript metadata columns are present.")
        engine = create_engine(f"sqlite:///{db_path}")
        try:
            ensure_transcript_metadata_columns(engine)
        finally:
            engine.dispose()

    logging.info("Import completed successfully.")
    return backup_destination


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Utility to export or import the RSS Analysis SQLite database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging.")
    parser.add_argument("--log-level", default="INFO", help="Logging level when --verbose is not set.")
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR, help="Directory for log outputs.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export", help="Export the database to a SQL dump.")
    export_parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="Path to the SQLite database.")
    export_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="File path where the SQL dump should be written. Defaults to stdout.",
    )

    import_parser = subparsers.add_parser("import", help="Import the database from a SQL dump.")
    import_parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="Path to the SQLite database.")
    import_parser.add_argument(
        "-i",
        "--input",
        type=Path,
        help="File path containing the SQL dump. Defaults to stdin.",
    )
    import_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing database file. Creates a timestamped backup unless --no-backup is supplied.",
    )
    import_parser.add_argument(
        "--backup",
        type=Path,
        help="Optional path for the backup copy taken before overwriting the database.",
    )
    import_parser.add_argument(
        "--no-upgrade",
        action="store_true",
        help="Skip post-import schema upgrade checks.",
    )

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    log_dir = Path(args.log_dir).expanduser().resolve()
    run_label = configure_logging(args.verbose, args.log_level, log_dir, args.command)

    try:
        if args.command == "export":
            export_database(db_path=Path(args.db), output_path=args.output)
            logging.info("Export completed (run=%s).", run_label)
        elif args.command == "import":
            backup_destination = import_database(
                db_path=Path(args.db),
                input_path=args.input,
                force=args.force,
                backup_path=args.backup,
                skip_upgrade=args.no_upgrade,
            )
            if backup_destination:
                logging.info("Previous database backed up at %s", backup_destination)
            logging.info("Import completed (run=%s).", run_label)
        else:
            parser.error("Unknown command")
    except Exception as exc:
        logging.error(str(exc))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
