# TikTok Research Tools

**A simple but powerful tool for collecting TikTok data for academic research, no coding required.**

This tool provides a browser-based interface for the [TikTok Research API](https://developers.tiktok.com/products/research-api/), making it straightforward to build systematic datasets from TikTok without needing to write any code. It was developed as part of the [Influencer Stories](https://influencerstories.bham.ac.uk/) project at the University of Birmingham by **Dr Alex Christiansen**.

An earlier, more limited version of the data collection approach implemented here was used in:

> Christiansen, A., Craythorne, S.-L., Crawford, P., Larkin, M., Gohil, A., Strutt, S., & Page, R. (2025). Multimodal Analysis of Stories Told by Mental Health Influencers on TikTok. *Health Expectations*, 28(3), e70226. https://doi.org/10.1111/hex.70226

---

## **Before You Start**

This tool requires an approved **TikTok Research API** account. This is a separate application process run by TikTok, available to academic researchers at eligible institutions. If you do not yet have API access, you can apply at the [TikTok Research API page](https://developers.tiktok.com/products/research-api/). Note that ethical approval from your institution is a standard requirement of the application process and that procuring access can take a while - I'd recommend starting this as early as possible in your project.

Once you have been approved, TikTok will provide you with a **client key** and **client secret**, two strings of characters that act as your login credentials for the API. You will need these to use this tool.

---

## Quick Start (Windows)

If you are on a Windows computer, getting started takes four steps:

**Step 1: Install Python**

Python is the programming language this tool runs on. If you do not already have it, download and install it from [python.org](https://www.python.org/downloads/). During installation, make sure to tick the box that says **"Add Python to PATH"** before clicking Install.

**Step 2: Download this tool**

Click the green **Code** button at the top of this page, then click **Download ZIP**. Unzip the downloaded file somewhere convenient, such as your Desktop or Documents folder.

**Step 3: Launch the tool**

Open the folder you just unzipped and double-click **run_tiktok_app.bat**. A small terminal window will appear briefly, and then the tool will open automatically in your web browser. You can minimise the terminal window but do not close it, it needs to stay running in the background to keep the tool working.

**Step 4: Enter your credentials**

In the sidebar on the left, enter your TikTok Research API client key and client secret. Optionally, you can click **💾** to save them. You will only need to do this once, as they will be remembered for future sessions. Your credentials are stored only on your own computer and are never shared with anyone (note that sharing your credentials with people outside the specified project team goes against the Research API Terms of Service and can get your access revoked).

You are now ready to collect data!

---

## Quick Start (Mac or Linux)

**Step 1: Install Python**

Download and install Python from [python.org](https://www.python.org/downloads/) if you do not already have it. Python 3.8 or higher is required.

**Step 2: Download this tool**

Click the green **Code** button at the top of this page, then click **Download ZIP**. Unzip the downloaded file somewhere convenient.

**Step 3: Install dependencies**

Open a Terminal, navigate to the folder you just unzipped, and run:

```
pip install streamlit requests
```

**Step 4: Launch the tool**

In the same Terminal window, run:

```
streamlit run tiktok_research_app.py
```

The tool will open automatically in your web browser.

**Step 5: Enter your credentials**

In the sidebar on the left, enter your TikTok Research API client key and client secret. Optionally, you can click **💾** to save them. You will only need to do this once, as they will be remembered for future sessions. Your credentials are stored only on your own computer and are never shared with anyone.

---

## What Can I Collect?

The tool is organised into five modes, selectable at the top of the interface.

### Collect Video Data

This is the core mode of the tool. You can collect video metadata in two ways:

**By account**: enter a list of TikTok usernames and the tool will collect all of their videos within a date range you specify. This is useful when you have identified a set of relevant accounts and want to build a complete dataset of their content. No keywords or hashtags are needed.

**By keyword or hashtag**: build a search query using combinations of keywords and hashtags, joined with AND, OR and NOT logic. This is more similar to how most social media research tools work.

The two approaches can also be combined, e.g. for collecting videos from a set of accounts that also mention a particular keyword. Additionally, a filter can be applied to ensure a minimum level of engagement (likes, shares, comments) to ensure that the data you collect is relevant for your research.

* Video data includes: post date, username, region, video description, auto-generated transcription (voice to text), music ID, like count, comment count, share count, view count, hashtags, AIGC (AI-generated content) labels, and more.
* Note that the TikTok API only allows queries covering 30 days at a time. If you select a longer date range, the tool handles this automatically by splitting it into monthly chunks.

### Collect Comment Data

Once you have collected video data, you can use this mode to collect the comments on those videos. You can load the video CSV you collected in the previous step, optionally sample it down to a manageable number of videos, and then fetch all available comments. Comment data includes the comment text, like count, timestamp, and reply structure, which allows comment threads to be reconstructed.

The tool shows an estimate of how many API requests a collection run will use before you start, so you can manage your daily allowance (1,000 requests per day, shared across all modes except follower/following requests).

### Collect User Data

Query user-level information for a list of accounts, including their pinned videos, liked videos, followers, and following lists.

### Playlist Info

Retrieve the metadata and full video ID list for one or more TikTok playlists, using the numeric playlist ID from the playlist URL.

### TikTok Shop

Query TikTok Shop data for shops operating in the EU, including shop details, product listings, and customer reviews. Can be run step by step or as a single workflow from shop name through to reviews.

---

## Saving Query Presets

If you have a query you want to run again in future, for example a particular combination of accounts or keywords, you can save it as a named preset using the sidebar. Presets are stored locally in a file called `tiktok_presets.json` in the same folder as the tool.

---

## API Rate Limits

The TikTok Research API allows **1,000 requests per day** (reset at midnight UTC), shared across all endpoints. Each request returns up to 100 records, giving a theoretical maximum of 100,000 records per day. The tool displays request estimates before collection runs where possible.

For followers and following endpoints, the daily limit is 20,000 requests.

---

## Terms of Service and Research Ethics

This tool collects data through TikTok's official research infrastructure meaning no scraping or unofficial access is used. Use of the tool requires compliance with the [TikTok Research API Terms of Service](https://developers.tiktok.com/doc/research-api-get-started), which include restrictions on how long data can be stored, prohibitions on re-identifying individual users, and limits on commercial use. You are responsible for ensuring your research complies with these terms.

You should also consult your institution's research ethics board regarding data handling, storage periods, and any applicable data protection regulations (including GDPR where relevant) before beginning data collection. Note that ethical approval is also a standard requirement of the TikTok Research API application process itself.

All data collected by this tool is saved locally to your own computer. Nothing is uploaded or shared remotely.

---

## Known Limitations

- **Metadata only**: the API provides video metadata but not video files or audio. The voice-to-text transcription field provides a text version of spoken content where available
- **Engagement metric discrepancies**: figures for views, likes and other metrics may differ from what is shown on the TikTok platform, as the API uses archived data. See [Pearson et al. (2024)](https://doi.org/10.1080/1369118X.2024.2420032) for a detailed audit of these discrepancies
- **Indexing delay**: new videos can take up to 48 hours to appear in API results, which limits applicability for real-time data collection
- **Unavailable accounts**: some accounts may return errors even when publicly visible. The tool identifies these automatically and continues collecting from the remaining accounts
- **TikTok Shop**: shop data is only available for shops operating in the EU

---
## Citing This Tool

If you use this tool in your research, please cite it as:

> Christiansen, A. (2025). *TikTok Research Tools* [Software]. University of Birmingham. https://github.com/drchristlex/tiktok-research-tools

A methods paper describing the account-centric corpus construction approach this tool supports is currently in preparation. A citation will be added here on publication.

---

## Licence

MIT License, see `LICENSE` for details.

---

## Contact

For questions, bug reports, or feedback, please open an issue on this repository or contact me at a.l.christiansen@bham.ac.uk
