# TikTok Research Tools

A browser-based interface for collecting data from the [TikTok Research API](https://developers.tiktok.com/products/research-api/), developed as part of the **Influencer Stories** project at the University of Birmingham.

Developed by **Dr Alex Christiansen**, University of Birmingham.

An earlier, more limited version of the data collection approach implemented here was used in:

> Christiansen, A., Craythorne, S.-L., Crawford, P., Larkin, M., Gohil, A., Strutt, S., & Page, R. (2025). Multimodal Analysis of Stories Told by Mental Health Influencers on TikTok. Health Expectations: An International Journal of Public Participation in Health Care and Health Policy, 28(3), e70226. [https://doi.org/10.1111/hex.70226](https://doi.org/10.1111/hex.70226)

## Overview

Most social media research tends to focus on keywords and hashtags as the only metrics for locating appropriate content, with engagement metrics occasionally serving as a filter. But This vastly underestimates the flexibility provided by the TikTok Research API. This tool provides a point-and-click interface for the full range of TikTok Research API endpoints, making systematic data collection accessible to researchers without extensive programming experience. Features include:

* Accessing video metadata such as AIGC tags, voice-to-text auto-transcription, music ID and other aspects unique to TikTok.
* Accessing comment data, including reply structures that allow recreation of comment threads
* Accessing TikTok shop data, including item IDs, item descriptions and item reviews

## Requirements

### TikTok Research API Access

This tool requires an approved TikTok Research API account. Access is available to academic researchers at eligible institutions. To apply, visit the [TikTok Research API page](https://developers.tiktok.com/products/research-api/).

**You must have your own approved API credentials before using this tool.** The tool does not provide or share API access.

### Software Requirements

- Python 3.12 or higher
- The following Python packages: `streamlit`, `requests`

Install dependencies with:

```
pip install streamlit requests
```

## Getting Started

### Windows

Double-click `run_tiktok_app.bat`. This will install any missing dependencies and open the app in your browser automatically.

### Mac / Linux

Open a terminal, navigate to the folder containing the script, and run:

```
streamlit run tiktok_research_app.py
```

### Entering Your Credentials

On first use, enter your TikTok Research API client key and client secret in the sidebar. You can click **Save credentials** to store them locally in a `.env` file so you do not need to re-enter them each session. Credentials are stored only on your machine and are never transmitted anywhere other than to TikTok's authentication endpoint. Make sure to store your credentials somewhere safe and to never share them with anyone.

## Features

The tool is organised into five modes, selectable at the top of the interface:

### Collect Video Data

Search for videos using a flexible query builder. Supports:
- **Account-centric collection** — enter a list of usernames to collect all videos from specific accounts within a date range, with no keyword filtering required
- **Keyword and hashtag queries** — build complex boolean queries using AND, OR and NOT logic across multiple term groups
- **Filters** — region, video length, view count, comment count

Note that date ranges longer than 30 days are automatically split into monthly chunks, as required by the API.

### Collect Comment Data

Collect comments for a list of videos. Accepts either a CSV produced by the video collection mode or an uploaded file. Includes:
- A request budget estimator based on actual comment counts from the video data
- Optional sampling (by view count, comment count, or random) to manage API usage
- Parallel fetching to speed up collection

### Collect User Data

Query user-level data for a list of accounts:
- **Pinned videos** — single request per user
- **Liked videos** — paginated
- **Followers / Following** — paginated (note: these endpoints have a higher daily limit of 20,000 requests)

### Playlist Info

Retrieve metadata and video IDs for one or more playlists by playlist ID.

### TikTok Shop

Query TikTok Shop data for EU-based shops, including shop info, products, and reviews. Can be run as individual steps or as a single sequential workflow from shop name through to reviews.

---

## Query Presets

Frequently used query configurations can be saved as named presets using the sidebar. Presets are stored locally in `tiktok_presets.json`.

## Output

All data is saved as CSV files to a location you specify. No data is uploaded, shared, or stored remotely.

---

## API Rate Limits

The TikTok Research API allows **1,000 requests per day** (reset at midnight UTC), shared across all endpoints. Each request returns up to 100 records, giving a theoretical maximum of 100,000 records per day. The tool displays request estimates before collection runs where possible.

For followers and following endpoints, the daily limit is 20,000 requests.

## Terms of Service and Ethics

Use of this tool requires compliance with the [TikTok Research API Terms of Service](https://developers.tiktok.com/doc/research-api-get-started). Key obligations include restrictions on data storage duration, prohibitions on re-identification of users, and limits on commercial use of collected data. Researchers are responsible for ensuring their use of this tool complies with TikTok's terms.

Researchers should also consult their institution's research ethics board regarding data handling, storage, and any applicable data protection regulations (including GDPR where relevant) before beginning data collection. Note that ethical approval is a default requirement of gaining access to the research API.

The tool collects only publicly available data through TikTok's official research infrastructure. No scraping or unofficial API access is used.

---

## Known Limitations

- The Research API returns metadata only — video files and audio cannot be collected
- Engagement metrics (views, likes etc.) may differ from figures shown on the TikTok platform, as the API uses archived rather than live data. See [Pearson et al. (2024)](https://doi.org/10.1080/1369118X.2024.2420032) for a detailed audit of these discrepancies
- New videos can take up to 48 hours to appear in API results, limiting the tools applicability in live data collection exercises
- Some accounts may return errors even when publicly visible; the tool handles these as well as it can and continues collection for remaining accounts - please do let me know if you notice any errors with the handling
- TikTok Shop data is only available for shops operating in the EU

## Citing This Tool

If you use this tool in your research, please cite both the software and the accompanying methods paper (forthcoming):

**Software:**
> Christiansen, A. (2025). *TikTok Research Tools* [Software]. University of Birmingham. https://github.com/drchristlex/tiktok-research-tools

**Methods paper:** *(forthcoming — citation will be added on publication)*

---

## Licence

MIT License — see `LICENSE` for details.
