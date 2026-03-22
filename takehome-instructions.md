# Weather Data Project

Your goal is to help stakeholders understand past weather in Downtown LA. They want answers to questions like:

1. What was the hottest day in the last 10 days, and how far above the daily normal was it?
2. Which day had the biggest swing between the high and low temperature?
3. How many of the last 10 days were warmer than average for this time of year?
4. Did any of the last 10 days set or come close to breaking an all-time record high or low?
5. What are the trends in the past 3 months?

---

## Dataset Overview

The `weather-pdfs` folder contains 10 PDFs, where one file is one per day of weather. Note that each PDF may be formatted differently and data may be inconsistent across files.

The `3month_weather.csv` contains the past 3 months of weather data for longer term analysis. Note that the csv is poorly formatted.

You can choose to utilize one or both in your analysis. Do note that the data from the past 10 days may or may not match the data from the past 3 months.

## Phase 1 — Data Ingestion & Storage

Parse all 10 PDFs and load the data into a structured database of your choice.

- Extract data programmatically — not manually
- Design a schema that could scale to more files
- Write a repeatable ingestion script
- Handle missing or inconsistent fields gracefully
- Document schema decisions and assumptions

---

## Phase 2 — Data Analysis

Surface insights using a Jupyter Notebook (pandas, matplotlib, seaborn, etc.) or Tableau — your choice.

- At least 3 distinct analyses or charts
- Data sourced from your database — not hardcoded
- Label axis, units, and sources clearly
- Brief written explanation per analysis

---

## Phase 3 — AI Chatbot

Use any chatbot or LLM of your choice and demonstrate it can answer questions grounded in the weather data.

- Platform is your choice (ChatGPT, Claude, custom, etc.)
- Demonstrate answers to at least 5 real questions
- Answers must be grounded in the data — not generic
- Explain how you connected it to your data source

---

## What to Submit

1. Code repo of your data ingestion pipeline
2. Database schema overview
3. Jupyter Notebook or Tableau workbook of your data analysis
4. Chatbot demo recording

---

## What to Expect

Upon submission, we will set up a 30-min walkthrough to discuss your project deliverables.

Have fun! ☀️
