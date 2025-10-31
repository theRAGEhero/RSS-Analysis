# Metagov Seminar Transcripts

This directory houses the raw `.txt` transcripts from Metagov seminars (short talks, workshops, panels, etc.). It also includes a lightweight analysis stack that extracts cross-talk themes and ships several interactive visualizations:

- `analysis/topic_map_pipeline.py` ingests every transcript, runs a topic model, and prepares data/visual outputs (network, heatmap, PCA scatter, term explorer, sunburst).
- `analysis_output/` stores the generated CSV summaries and HTML dashboards, including the all-in-one viewer located at `analysis_output/visualizations/topic_dashboard.html`.

## Refreshing the insights

1. Activate the virtualenv (`source .venv/bin/activate`) or reinstall dependencies with `pip install -r analysis/requirements.txt`.
2. Run the pipeline: `python analysis/topic_map_pipeline.py --input-dir . --output-dir analysis_output`.
3. Open the updated HTML files (especially `topic_dashboard.html`) in a browser to explore the refreshed maps.

Whenever you drop new transcripts into this directory, rerun the script—everything is picked up automatically, so the visuals evolve along with the archive.
