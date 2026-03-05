from __future__ import annotations
import json
from typing import Dict, List


def build_comparison_prompt(
    latest_invoice: Dict,
    previous_month_invoice: Dict,
    avg_grand_total: float,
    avg_net_total: float,
    avg_tax: float,
    avg_rental: float,
) -> str:

    return f"""You are a senior telecom billing analyst preparing a formal invoice comparison report for internal finance and operations leadership.

CONTEXT:
- This is an internal business report, not a conversational response
- The report will be stored, exported to PDF, and reviewed by management

LATEST INVOICE (Current):
{json.dumps(latest_invoice, indent=2, default=str)}

PREVIOUS MONTH INVOICE:
{json.dumps(previous_month_invoice, indent=2, default=str)}

6-MONTH HISTORICAL AVERAGES:
- Average Grand Total: {avg_grand_total:,.2f}
- Average Net Total: {avg_net_total:,.2f}
- Average Tax: {avg_tax:,.2f}
- Average Rental: {avg_rental:,.2f}

ABSOLUTE OUTPUT RULES (MANDATORY):
- DO NOT include any introduction, greeting, preface, or closing remarks
- DO NOT include phrases like "Of course", "Here is", "This report shows"
- DO NOT include titles, dates, account headers, or decorative separators
- DO NOT use markdown emphasis such as **bold**, *, _, or ***
- DO NOT use horizontal rules (---)
- DO NOT invent explanations beyond the data provided
- DO NOT use currency symbols; use numeric values only
- DO NOT repeat raw JSON or restate the input

FORMATTING RULES:
- Use markdown headers (##) ONLY for section headings
- Tables must be plain markdown tables without bold formatting
- Percentages must be numeric only (e.g., -32.20%)
- Use clear, professional, neutral business language
- Base ALL calculations strictly on provided values

REPORT STRUCTURE (FOLLOW EXACTLY):

## 1. Executive Summary
- 3–4 sentences summarizing the key financial movement
- Focus on the largest variance in Grand Total
- Clearly state whether the change is increase or decrease and why

## 2. Financial Comparison

### Month-over-Month Comparison
Present a table with the following columns:
Metric | Current Month | Previous Month | Change | Change %

Include:
- Grand Total
- Net Total
- Total Tax
- Rental Charge

Calculations:
- Change = Current - Previous
- Change % = (Change / Previous) × 100

### Six-Month Benchmark Comparison
Metric | Current Month | Six-Month Average | Variance | Variance %

Include:
- Grand Total

## 3. Service and Configuration Review
Compare the following fields and state clearly if they changed or not:
- Service Name
- Bandwidth
- Provider
- Cost Center
- Charge Per Minute

## 4. Root Cause Analysis
Explain the primary drivers of the variance:
- Service configuration changes
- Usage changes
- Rate or rental changes
- Tax changes
- Credits or adjustments

Only list causes supported by the data.

## 5. Recommendations
Provide 3–5 clear, actionable recommendations based on the findings.

## 6. Risk Assessment
Classify the invoice as:
- Low Risk: variance < 5%
- Medium Risk: variance 5–15%
- High Risk: variance > 15%

Explain the classification in 2–3 sentences.

LENGTH REQUIREMENTS:
- 500–700 words
- Concise, structured, factual

Generate the report now:"""