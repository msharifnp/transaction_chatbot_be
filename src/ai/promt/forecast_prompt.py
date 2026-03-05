from __future__ import annotations
from typing import Dict, List


def build_forecast_summary_prompt(
    user_query: str,
    forecast_data: List[Dict],
    validated_spec: Dict,
    periods: int = 12,
) -> str:

    date_column = validated_spec.get("date_column", "date")
    value_columns = validated_spec.get("value_columns", [])
    grouping_columns = validated_spec.get("grouping_columns", [])
    time_bucket = validated_spec.get("time_bucket", "month")
    forecast_periods = validated_spec.get("forecast_periods", periods)
    has_groups = "group" in (forecast_data[0].keys() if forecast_data else [])

    return f"""You are a senior financial forecasting analyst specializing in telecom invoice prediction.

CONTEXT:
- Historical Records: {len(forecast_data)} aggregated data points
- Date Column: {date_column}
- Value Columns to Forecast: {', '.join(value_columns)}
- Time Granularity: {time_bucket}
- Forecast Horizon: {forecast_periods} {time_bucket}s
{'- Grouped By: ' + ', '.join(grouping_columns) if grouping_columns else '- No grouping (single forecast)'}

USER REQUEST: "{user_query}"

ABSOLUTE OUTPUT RULES:
- DO NOT include any title, introduction, greeting, explanation, preface, or metadata
(e.g., no "Of course", no To/From/Date/Subject, no *** separators).

FORECAST REQUIREMENTS:

1. Data Understanding
- Analyze the time series pattern (trend, seasonality, volatility)
- Identify any anomalies or outliers in historical data
- Comment on data quality and completeness

2. Forecast Generation
- Generate {forecast_periods} future {time_bucket} predictions for each value column
- Use appropriate forecasting method based on the pattern (trend-based, seasonal, or simple moving average)
- {'Generate separate forecasts for each group' if has_groups else 'Generate single unified forecast'}

3. Business Insights
- Explain the trend direction and magnitude
- Highlight potential risks or opportunities
- Provide confidence assessment

CRITICAL OUTPUT FORMAT:
Return a well-structured markdown report with:

## Forecast Summary
Brief overview of predictions and key findings (2-3 sentences)

## Historical Analysis
- Pattern observed: [trend description]
- Data quality: [assessment]
- Notable observations: [key points]

## Predictions
Present forecast in a clear table format:

| Period | {' | '.join(value_columns)} | Confidence |
|--------|{('--------|' * len(value_columns))}-----------|
| 2025-10 | [values] | High/Medium/Low |
...

## Key Insights
- trend: [increasing/decreasing/stable - describe the overall trend]
- seasonality: [detected/not_detected - describe seasonal patterns if any]
- key_drivers: [factor 1, factor 2, factor 3]
- confidence_level: [high/medium/low - overall confidence in predictions]
- notes: [Any important observations about the forecast]

## Recommendations
1. [Lock vendor pricing]
2. [Monitor high-growth sites]
3. [Any specific action based on forecast]

## Risk Factors
- Risk 1: [Potential seasonal spike]
- Risk 2: [Vendor dependency risk]
- Risk 3: [Any other potential risk]

FORMATTING RULES:
- Use markdown headers (##) for sections
- Use **bold** for key numbers and metrics
- Include specific values and percentages
- Keep professional business tone
- Base ALL statements on the provided data
- Confidence values: "High" (clear pattern), "Medium" (some uncertainty), "Low" (high volatility)
- Date format: YYYY-MM-DD
- Total length: 400-600 words

Generate the comprehensive forecast report now:"""


def build_forecast_chart_prompt(
    user_query: str,
    data_block: str,
    size: str = "960x560",
) -> str:

    return f"""You are an expert data visualization specialist. Create a CLEAN, PROFESSIONAL forecast line chart.

# ABSOLUTE REQUIREMENTS:

## Chart Structure
- Type: Multi-line chart (one line per group/account)
- X-axis: Time (dates/months) - chronologically sorted
- Y-axis: Net Total ($) values
- Historical data: SOLID lines
- Forecast data: DASHED lines (stroke-dasharray="6,4")
- Vertical separator: Dashed line at forecast start point

## Visual Design - FOLLOW EXACTLY
- Canvas: {size}
- Background: #0f172a (dark blue-gray)
- Line width: 2.5px
- **Line style: STRAIGHT segments between points** (use simple line-to commands, NOT bezier curves)
- Data point markers: Circles, radius 5px, filled with line color
- Grid lines: Horizontal only, color #1e293b, subtle

## Colors (Use these EXACT colors)
- Line 1 (IVP001): #3b82f6 (blue)
- Line 2 (IVP002): #10b981 (green)
- Line 3 (IVP003): #f59e0b (orange)
- Additional lines: #ef4444 (red), #8b5cf6 (purple), #06b6d4 (cyan)

## Axes Formatting
**X-axis:**
- Labels: "Jan 2025", "Feb 2025", etc. (month name + year)
- Show every month or every 2 months
- Font: 12px, color #94a3b8
- Position: Bottom of chart

**Y-axis:**
- Format: Numbers with commas (e.g., "10,000" not "10000")
- Font: 12px, color #94a3b8
- Position: Left side
- Title: "Predicted Net Total ($)" - rotated -90°, position on left

## Legend - TOP LEFT CORNER (CRITICAL)
- Position: Top-left corner (x: 80, y: 40)
- Each series: Colored circle + label text
- Layout: Vertical stack OR horizontal row
- Font: 13px, color #e2e8f0
- Include: All group names (IVP001, IVP002, IVP003, etc.)
- Add separator labels: "Historical" (solid line icon) and "Forecast" (dashed line icon)

## Data Point Labels
- Show values at LAST historical point and FIRST 2-3 forecast points
- Position: Above the marker
- Font: 11px, color matching line
- Format: With commas

## Forecast Demarcation
- Vertical dashed line at the boundary between historical and forecast
- Color: #475569
- Style: stroke-dasharray="4,4"
- Optional label: "Forecast →" at top

## CRITICAL RULES
1. **NO smooth curves** - use straight line segments (SVG `L` command, not `C` or `Q`)
2. **NO overlapping lines** - each group must be clearly distinguishable
3. **Consistent data points** - every point gets a marker circle
4. **Clean spacing** - adequate padding (left: 70px, right: 40px, top: 60px, bottom: 50px)
5. **Sorted data** - ensure dates are chronological before plotting
6. **Legend in TOP LEFT** - not top right, not bottom

## Output Format
- Output ONLY the SVG code
- NO markdown backticks (```)
- NO explanatory text
- NO comments in the SVG
- Start with `<svg` and end with `</svg>`
- Valid, well-formed XML

# USER REQUEST: "{user_query}"

{data_block}

# IMPLEMENTATION STEPS:
1. Parse data and identify unique groups
2. Sort all data points by date chronologically
3. Calculate axis scales (min/max values with padding)
4. Draw grid lines (horizontal, subtle)
5. Draw Y-axis with formatted labels
6. Draw X-axis with month labels
7. For each group:
   - Plot line using straight segments (L commands)
   - Add circle markers at each point
   - Use solid stroke for historical, dashed for forecast
8. Add vertical separator at forecast boundary
9. Draw legend in TOP LEFT corner with all groups
10. Add selected data point labels

Generate the clean forecast chart now (SVG only, no other text):"""