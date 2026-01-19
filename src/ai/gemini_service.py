import os
import re
import json
import base64
import csv
from io import StringIO
from typing import Dict, List, Optional, Tuple
import time
from src.config.field_constant import SYSTEM_INSTRUCTION_SVG, UI_COLUMNS
from src.config.field_constant import FIELD_TYPES
from src.ai.chart_spec_generator import SpecGenerator
from src.aggregator.chart_aggregator import aggregate_rows
from src.utils.utils import retry_with_backoff,choose_optimal_format,restore_original_columns,get_summary_spec
from src.db.model_service import ModelService
from src.ai.forecast_spec_generator import ForecastSpecGenerator
from src.aggregator.forecast_aggregator import prepare_forecast_data,validate_and_fix_forecast_spec
from src.ai.summary_spec_generator import SummarySpecGenerator
from src.aggregator.summary_aggregator import aggregate_for_summary


class GeminiService:
    
    def __init__(self, model_service: ModelService):
        self.model_service = model_service
        self.enabled = model_service.is_available()
    
    def generate_summary_1(self, user_query: str, rows: List[Dict]) -> str:
        """Generate executive summary report using Gemini."""
        if not self.enabled:
            print("[SUMMARY] Gemini disabled")
            return "⚠️ AI summary generation is currently disabled."

        if not rows:
            print("[SUMMARY] No rows")
            return "⚠️ No data available for summary."

        print(f"[SUMMARY] 📊 Starting summary generation with {len(rows)} rows")
        print(f"[SUMMARY] User query: {user_query}")

        try:
            # Step 1: Format data
            print("[SUMMARY] Step 1: Formatting data...")
            format_type, formatted_data = choose_optimal_format(rows, "summary")
            data_block = f"DATA ({'CSV' if format_type=='csv' else 'JSON'}):\n```{format_type}\n{formatted_data}\n```"
            
            print(f"[SUMMARY] ✅ Data formatted as {format_type.upper()} ({len(formatted_data)} chars)")
            
            # Step 2: Extract metadata
            num_records = len(rows)
            sample_cols = list(rows[0].keys()) if rows else []

            # Detect date range if invoice_date exists
            date_info = ""
            if "invoice_date" in sample_cols:
                try:
                    dates = [
                        r.get("invoice_date") for r in rows if r.get("invoice_date")
                    ]
                    if dates:
                        date_info = f"\n- Date Range: {min(dates)} to {max(dates)}"
                except:
                    pass

            print(f"[SUMMARY] Records: {num_records}, Columns: {len(sample_cols)}")
            if date_info:
                print(f"[SUMMARY] {date_info.strip()}")

            # Step 3: Build summary prompt
            prompt = f"""You are a senior business intelligence analyst preparing an executive summary report for management.

CONTEXT:
- Dataset: Invoice / Billing / Transaction Records
- Total Records:  invoices{date_info}
- Data Type: Pre-aggregated summary with detailed status breakdowns
- Always be clear, structured, and professional

USER REQUEST: "{user_query or 'Provide a comprehensive business summary and insights from this dataset.'}"

{data_block}

ANALYSIS REQUIREMENTS:

ABSOLUTE OUTPUT RULES:
- DO NOT include any title, introduction, greeting, explanation, preface, or metadata
  (e.g., no "Of course", no To/From/Date/Subject, no *** separators)

Generate a professional business report aimed at finance and operations leaders with the following sections:

## 1. Executive Summary (2–4 sentences)
- Briefly describe the dataset scope using aggregations.time.date_range
- Highlight the single most important business finding from the aggregated data
- Reference key metrics from aggregations.financial (total_billed, total_invoices)

## 2. Status & Risk Overview
**CRITICAL: Use the enhanced risk breakdown structure**

A) **Disputed Invoices Analysis**
- Use aggregations.risk.disputed_breakdown
- Show the summary: total_count, total_amount, percentage
- Analyze the breakdown table showing:
  - InvoiceStatusType (Disputed vs System Disputed)
  - InvoiceApprovalStatus (what stage they're stuck at)
  - PaymentStatus (payment readiness)
  - VerificationResult (verification status)
- Identify the most common combination causing issues
- Quantify the value impact of disputes

B) **Accepted Invoices Analysis**
- Use aggregations.risk.accepted_breakdown
- Show the summary: total_count, total_amount, percentage
- Analyze the breakdown table showing:
  - InvoiceStatusType (Accepted vs System Accepted)
  - InvoiceApprovalStatus (approval progress)
  - PaymentStatus (payment status)
  - VerificationResult (verification completion)
- Identify how many are fully processed (Verified + Approval Completed + Settled)
- Note any accepted invoices with incomplete processing

C) **Other Risk Indicators**
- Use aggregations.risk.not_verified (if present)
  - Count, total_amount, percentage of unverified invoices
- Use aggregations.risk.pending_approval (if present)
  - Count, total_amount, percentage of pending approvals

D) **Overall Risk Assessment**
- Calculate total at-risk value (disputed + not verified + pending)
- Comment on operational risk implications
- Identify process bottlenecks from the detailed breakdowns

## 3. Provider & Service Analysis
- Use aggregations.provider to identify top providers by total_amount
- Calculate vendor dependency (top provider's share of aggregations.financial.total_billed)
- If aggregations.service exists, summarize main service types
- Comment on concentration vs. diversification

## 4. Cost Center & Location View
- Use aggregations.cost_center to list top locations by total_amount
- Calculate spending concentration (top 3 share of total)
- Cross-reference with risk data if location-specific patterns exist
- Note any concentration issues

## 5. Financial Snapshot
Use aggregations.financial for all metrics:
- **Total Billed Value**: total_billed
- **Average Invoice Value**: average_invoice
- **Invoice Range**: min_invoice to max_invoice
- **Median Invoice**: median_invoice
- **Total Tax**: total_tax (if available)
- **Net Total**: total_net (if available)
- **Total Usage**: total_usage (if available)
- **Total Rental**: total_rental (if available)
Interpret what these numbers mean for spending patterns

## 6. Operational & Process Insights
- Analyze aggregations.risk.accepted_breakdown and aggregations.risk.disputed_breakdown together
- Calculate processing efficiency:
  - % of invoices fully processed (Verified + Approval Completed)
  - % stuck in approval workflow
  - % with verification issues
- Identify bottlenecks in approval/verification process from breakdown patterns
- Note any patterns where specific status combinations are common

## 7. Risk Factors & Red Flags
- Use aggregations.risk.disputed_breakdown.summary for total disputed exposure
- Highlight most problematic status combinations from the breakdowns
- Use aggregations.status to identify unusual patterns
- Call out high-value outliers in aggregations.provider or aggregations.cost_center
- Note data quality issues if evident

## 8. Key Recommendations (3–6 bullet points)
- Provide concrete, action-oriented recommendations
- Prioritize based on aggregations.risk data (focus on highest-value issues)
- Reference specific status combinations that need attention
- Suggest process improvements based on breakdown patterns
- Include cost optimization opportunities from provider/cost_center data
- Recommend data quality fixes if needed

FORMATTING RULES:
- Use markdown headers (##) for sections exactly as above
- Use **bold** for key metrics and numbers
- Always include specific numbers and percentages
- Format large numbers with commas (e.g., "1,234,567")
- Do NOT invent currency symbols; use numeric values only unless currency is explicit
- Keep each section concise (3–5 sentences or bullet points)
- Total length: roughly 500–700 words
- Maintain formal, professional business tone

CRITICAL RULES FOR RISK SECTION:
- aggregations.risk.disputed_breakdown has TWO parts:
  1. summary: {{"total_count": X, "total_amount": Y, "percentage": Z}}
  2. breakdown: [{{"InvoiceStatusType": "...", "InvoiceApprovalStatus": "...", ...}}]
- aggregations.risk.accepted_breakdown has the SAME structure
- ALWAYS analyze BOTH the summary AND the breakdown table
- Compare patterns between disputed and accepted invoices
- Base ALL statements strictly on the aggregated data provided
- NEVER make assumptions or add information not in the aggregations
- If data is missing for a section, state: "Insufficient data for [section name] analysis."
- Always compare and contrast (e.g., "Disputed: 25% vs Accepted: 75%")  

    Generate the comprehensive report now:"""


            # Step 4: Call model and return result
            print("[SUMMARY] Step 2: Generating summary report...")
            
            def generate():
                text = self.model_service.generate_text(prompt)
                if not text or not text.strip():
                    raise RuntimeError("Empty response from model")
                return text.strip()

            result_text = retry_with_backoff(generate)
            
            if not result_text:
                return "⚠️ No summary could be generated from the data."

            print(f"[SUMMARY] ✅ Summary generated successfully ({len(result_text)} chars)")
            
            return result_text

        except Exception as e:
            print(f"[SUMMARY] ❌ Error: {e}")
            import traceback
            traceback.print_exc()
            return f"⚠️ Error generating summary: {str(e)}"
         
    def generate_forecast(self, user_query: str, rows: List[Dict], periods: int = 12) -> str:  
        """Generate forecast using Gemini with intelligent data preparation."""
        if not self.enabled:
            print("[FORECAST] Gemini disabled")
            return "⚠️ AI forecasting is currently disabled."

        if not rows:
            print("[FORECAST] No rows")
            return "⚠️ No data available for forecasting."

        print(f"[FORECAST] 📈 Starting forecast generation with {len(rows)} rows")
        print(f"[FORECAST] User query: {user_query}")

        try:
            # Step 1: Generate forecast spec
            print("[FORECAST] Step 1: Generating forecast spec...")
            spec_gen = ForecastSpecGenerator(self.model_service)
            available_columns = list(rows[0].keys())
            sample_rows = rows[:3]
            
            spec = spec_gen.generate_spec(
                user_query=user_query,
                available_columns=available_columns,
                field_types=FIELD_TYPES,
                sample_rows=sample_rows
            )
            
            print(f"[FORECAST] ✅ Spec generated: {json.dumps(spec, indent=2)}")

            # Step 2: Prepare forecast data using aggregator
            print(f"[FORECAST] Step 2: Preparing {len(rows)} rows for forecasting...")
            forecast_data, validated_spec = prepare_forecast_data(
                rows=rows,
                spec=spec,
                field_types=FIELD_TYPES,
                max_groups=50
            )
            
            
            
            
            print(f"[FORECAST] ✅ Prepared {len(forecast_data)} records")
           
            print(f"[FORECAST] ✅ Validated spec: {json.dumps(validated_spec, indent=2)}")
            
            if not forecast_data:
                return "⚠️ No valid data for forecasting. Please check your date and value columns."

            # Step 3: Format prepared data for model
            print("[FORECAST] Step 3: Formatting data...")
            format_type, formatted_data = choose_optimal_format(forecast_data, "forecast")
            data_block = f"DATA ({'CSV' if format_type=='csv' else 'JSON'}):\n```{format_type}\n{formatted_data}\n```"
            print(f" data for forecast {data_block}")
            
            
            
            chart_data = restore_original_columns(forecast_data, validated_spec)
            # format_type_chart, formatted_data_chart = choose_optimal_format(chart_data_rows, "forecast")
            # chart_data_block = f"DATA ({'CSV' if format_type_chart=='csv' else 'JSON'}):\n```{format_type_chart}\n{formatted_data_chart}\n```"
           
            
            # print(f"chart data block for chart : {chart_data_block}")
            
            
            print(f"[FORECAST] ✅ Data formatted as {format_type.upper()} ({len(formatted_data)} chars)")
           

            # Step 4: Extract info from validated spec for prompt
            date_column = validated_spec.get("date_column", "date")
            value_columns = validated_spec.get("value_columns", [])
            grouping_columns = validated_spec.get("grouping_columns", [])
            time_bucket = validated_spec.get("time_bucket", "month")
            forecast_periods = validated_spec.get("forecast_periods", periods)
            has_groups = "group" in (forecast_data[0].keys() if forecast_data else [])
            
            print(f"[FORECAST] Forecasting: {value_columns} by {time_bucket}")
            if has_groups:
                print(f"[FORECAST] With grouping by: {grouping_columns}")

            # Step 5: Build forecast prompt (markdown output, not JSON)
            prompt = f"""You are a senior financial forecasting analyst specializing in telecom invoice prediction.

    CONTEXT:
    - Historical Records: {len(forecast_data)} aggregated data points
    - Date Column: {date_column}
    - Value Columns to Forecast: {', '.join(value_columns)}
    - Time Granularity: {time_bucket}
    - Forecast Horizon: {forecast_periods} {time_bucket}s
    {'- Grouped By: ' + ', '.join(grouping_columns) if grouping_columns else '- No grouping (single forecast)'}

    USER REQUEST: "{user_query}"

    {data_block}

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
    - key_drivers: [factor 1", "factor 2", "factor 3]
    - confidence_level: [high/medium/low - overall confidence in predictions ]
    - notes: [Any important observations about the forecast]

    ## Recommendations
    1. [Lock vendor pricing]
    2. [Monitor high-growth sites]
    3. [Any Specific action based on forecast]

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

            # Step 6: Call model and return result
            print("[FORECAST] Step 6: Generating forecast predictions...")
            
            def generate():
                text = self.model_service.generate_text(prompt)
                if not text or not text.strip():
                    raise RuntimeError("Empty response from model")
                return text.strip()

            result_text = retry_with_backoff(generate)
            
            if not result_text:
                return "⚠️ No forecast could be generated from the data."
            
            print(f"[FORECAST] ✅ Forecast generated successfully ({len(result_text)} chars)")
            
            return {
                "text":result_text,
                "forecast_rows":chart_data
            }

        except Exception as e:
            print(f"[FORECAST] ❌ Error: {e}")
            import traceback
            traceback.print_exc()
            return f"⚠️ Error generating forecast: {str(e)}"
    
    def generate_chart(self, user_query: str, rows: List[Dict], size: str = "960x560") -> Optional[Dict]:
        """Generate an SVG chart using Gemini with intelligent aggregation."""
        if not self.enabled:
            print("[CHART] Gemini disabled")
            return None

        if not rows:
            print("[CHART] No rows")
            return None

        print(f"[CHART] 📊 Starting chart generation with {len(rows)} rows")
        print(f"[CHART] User query: {user_query}")

        try:
            # Step 1: Generate aggregation spec
            print("[CHART] Step 1: Generating aggregation spec...")
            spec_gen = SpecGenerator(self.model_service)
            available_columns = list(rows[0].keys())
            sample_rows = rows[:3]  # Send 3 sample rows for context
            
            spec = spec_gen.generate_spec(
                user_query=user_query,
                task="chart",
                available_columns=available_columns,
                field_types=FIELD_TYPES,
                sample_rows=sample_rows,
                chart_hint=None  # Let model decide based on query
            )
            
            print(f"[CHART] ✅ Spec generated: {json.dumps(spec, indent=2)}")

            # Step 2: Aggregate the data using pandas
            #print(f"[CHART] Step 2: Aggregating {len(rows)} rows...")
            agg_rows = aggregate_rows(rows, spec, FIELD_TYPES, max_groups=100)
            print(f"[CHART] ✅ Aggregated :  {agg_rows} ")
            
            if not agg_rows:
                return {
                    "image_b64": None,
                    "image_mime": None,
                    "error": "⚠️ No data after aggregation. Please adjust your query."
                }

            # Step 3: Format aggregated data for chart generation
            print("[CHART] Step 3: Formatting data...")
            format_type, formatted_data = choose_optimal_format(agg_rows, "chart")
            data_block = f"DATA ({'CSV' if format_type=='csv' else 'JSON'}):\n```{format_type}\n{formatted_data}\n```"
            
            print(f" data for chart {data_block}")
            print(f"[CHART] ✅ Data formatted as {format_type.upper()} ({len(formatted_data)} chars)")

            # Step 4: Detect chart type from spec
            chart_type = spec.get("chart_type", "bar")
            print(f"[CHART] Chart type: {chart_type}")

            # Step 5: Generate SVG chart
            print("[CHART] Step 4: Generating SVG visualization...")
            
            prompt = f"""You are an expert data visualization specialist. Your task is to analyze the user's request and data, then create the most appropriate chart type.

    # CHART TYPE SELECTION RULES:
    # - Analyze the data structure and user query to determine the best visualization
    # - Consider: time series → line/area chart, comparisons → bar/column chart, proportions → pie chart, correlations → scatter plot, distributions → histogram, hierarchies → treemap, financial data → candlestick, density → heatmap
    # - Choose the chart type that best communicates the insight the user is seeking

    # CRITICAL REQUIREMENTS FOR ALL CHARTS:
    # 1. **Clean, Readable Axes:**
    # - X-axis: Clear labels, proper spacing, readable font (12-14px)
    # - Y-axis: Clean numeric values with appropriate formatting (e.g., 1K, 1M for large numbers)
    # - Both axes must have visible tick marks and grid lines (subtle)
    # - Axis titles should be descriptive and positioned clearly

    # 2. **Data Point Labels:**
    # - For bar/column charts: Show values on top/end of bars
    # - For line charts: Show values at data points (optional if too dense)
    # - For pie charts: Show percentages and labels clearly
    # - Use contrasting colors for readability against dark background

    # 3. **Formatting Standards:**
    # - Numbers: Format with commas (e.g., 1,234,567)
    # - Currency: Use $ symbol and appropriate decimals
    # - Percentages: Show with % symbol
    # - Dates: Use clear format (Jan 2024, Q1 2024, etc.)

    # 4. **Legend Requirements:**
    # - Include legend when multiple series/categories exist
    # - Position: top-right or bottom, never overlapping data
    # - Use clear, descriptive labels
    # - Match colors exactly to chart elements

    # 5. **Visual Design:**
    # - Background: #0f172a (dark slate)
    # - Text color: #ffffff (white) or #e2e8f0 (light gray) for readability
    # - Grid lines: #334155 (subtle, semi-transparent)
    # - Use high-contrast color palette for series:
    #     * Primary: #3b82f6 (blue)
    #     * Secondary: #10b981 (green)
    #     * Tertiary: #f59e0b (amber)
    #     * Additional: #ef4444 (red), #8b5cf6 (purple), #06b6d4 (cyan)
    # - Ensure all elements are clearly visible on dark background

    # 6. **Chart-Specific Requirements:**
    
    # **Line/Area Charts:**
    # - Smooth curves preferred
    # - Data points marked with circles
    # - Clear trend lines
    # - Shaded area for area charts (semi-transparent)
    # - ex: line chart of net total and rental charge against cost name , it should have 2 line, if they mention n number , then n lines
    
    # **Bar/Column Charts:**
    # - Adequate spacing between bars
    # - Values displayed on bars
    # - Bars should be proportional to values
    
    # **Pie Charts:**
    # - Show both percentage and value
    # - Labels with leader lines if needed
    # - Start at 12 o'clock position
    # - slice should be proper traingle
    # - Ensure slices are proper wedge shapes (center-to-arc triangular wedge)
    
    # **Scatter Plots:**
    # - Point size proportional to third dimension if applicable
    # - Include trend line if correlation exists
    # - Clear axis ranges
    
    # **Histograms:**
    # - Equal bin widths
    # - Show frequency/count
    # - Clear bin labels
    
    # **Bubble Charts:**
    # - Size legend showing bubble scale
    # - No overlapping bubbles if possible
    # - Clear labels for key bubbles
    
    # **Waterfall Charts:**
    # - Color code increases (green) and decreases (red)
    # - Show running total
    # - Connect bars with lines
    
    # **Bullet Graphs:**
    # - Clear performance bands
    # - Target marker visible
    # - Actual value prominently displayed
    
    # **Treemaps:**
    # - Hierarchical labels
    # - Size proportional to values
    # - Color intensity or categories
    
    # **Funnel Charts:**
    # - Stages clearly labeled
    # - Show conversion percentages
    # - Color gradient or distinct colors per stage
    
    # **Candlestick Charts:**
    # - Green for up days, red for down days
    # - Clear OHLC markers
    # - Date axis properly formatted
    # - Volume bars if applicable
    
    # **Heatmaps:**
    # - Color scale legend
    # - Row and column labels
    # - Gradient from cool to warm colors
    # - Cell values displayed if space permits
    
    # **Density Maps:**
    # - Clear color gradient legend
    # - Darker = higher density
    # - Contour lines if applicable

    # 7. **Accessibility:**
    # - Minimum font size: 12px
    # - High contrast ratios
    # - No color-only encoding (use patterns/shapes too if critical)

    # OUTPUT RULES:
    # - Output ONLY the SVG code
    # - No markdown code fences (no ```)
    # - No explanatory text before or after
    # - Start directly with <svg and end with </svg>
    # - SVG must be valid and self-contained
    # - Use the specified size exactly

    # USER REQUEST: "{user_query}"

    # {data_block}

    
    # CHART SPECIFICATIONS:
    # - Canvas Size: {size}
    # - Background Color: #0f172a
    # - Ensure all axes are clearly labeled with titles
    # - Format all numeric values appropriately (commas for thousands, K/M for large numbers)
    # - Display data point values where appropriate
    # - Include legend if multiple series exist
    # - Use high-contrast colors for dark background
    # - All text must be readable (minimum 12px)

    # TASK:
    # 1. Analyze the user request and data structure
    # 2. Select the most appropriate chart type (line, bar, column, pie, scatter, area, histogram, bubble, waterfall, bullet, treemap, funnel, candlestick, heatmap, density, or other)
    # 3. Create a clean, professional visualization with properly formatted axes and clear labels
    # 4. Output ONLY the SVG code with no additional text, explanations, or markdown formatting

    Generate the chart now:"""

            def generate_svg():
                text = self.model_service.generate_text(prompt)
                if not text or not text.strip():
                    raise RuntimeError("Empty SVG response from Gemini")
                return text.strip()

            # Generate with retry
            raw = retry_with_backoff(generate_svg, max_retries=3, initial_delay=2)

            if not raw:
                print("[CHART] ⚠️ Empty response from model")
                return {
                    "image_b64": None,
                    "image_mime": None,
                    "error": "⚠️ Chart generation returned no content. Please try again."
                }

            # Clean up response
            raw = raw.strip()
            raw = re.sub(r'^```[\w]*\s*', '', raw, flags=re.MULTILINE)
            raw = re.sub(r'\s*```\s*$', '', raw, flags=re.MULTILINE)
            raw = raw.strip()

            # Extract SVG
            svg_start = re.search(r'<svg[\s>]', raw, re.IGNORECASE)
            svg_end = re.search(r'</svg\s*>', raw, re.IGNORECASE)
            
            if not svg_start or not svg_end:
                print(f"[CHART] ❌ No valid SVG tags found")
                print(f"[CHART] Preview: {raw[:300]}")
                return {
                    "image_b64": None,
                    "image_mime": None,
                    "error": "⚠️ Chart generation failed - invalid SVG. Please try again."
                }

            svg = raw[svg_start.start():svg_end.end()].strip()

            # Validate
            if len(svg) < 100:
                print(f"[CHART] ❌ SVG too short ({len(svg)} chars)")
                return {
                    "image_b64": None,
                    "image_mime": None,
                    "error": "⚠️ Chart generation produced incomplete output."
                }

            if not svg.lower().startswith('<svg') or not svg.lower().endswith('</svg>'):
                print("[CHART] ❌ Invalid SVG format")
                return {
                    "image_b64": None,
                    "image_mime": None,
                    "error": "⚠️ Chart generation produced malformed SVG."
                }

            # Encode to base64
            b64 = base64.b64encode(svg.encode("utf-8")).decode("ascii")
            print(f"[CHART] ✅ Chart generated successfully ({len(svg)} bytes, {len(agg_rows)} data points)")
            
            return {
                "image_b64": b64,
                "image_mime": "image/svg+xml",
                "aggregation_spec": spec,  # Include spec for debugging
                "data_points": len(agg_rows)
            }

        except Exception as e:
            error_msg = str(e)
            print(f"[CHART] ❌ Generation failed: {e}")
            import traceback
            traceback.print_exc()

            return {
                "image_b64": None,
                "image_mime": None,
                "error": f"⚠️ Chart generation error: {error_msg}"
            }

    def generate_general_qa(self, user_query: str, rows: List[Dict]) -> str:
        """Answer ad-hoc analytical questions on invoice data using Gemini."""
        
        if not self.enabled:
            print("[QA] Gemini disabled")
            return "⚠️ AI analysis is currently disabled."

        if not rows:
            print("[QA] No rows")
            return "⚠️ No data available to answer the question."

        print(f"[QA] ❓ Starting general QA with {len(rows)} rows")
        print(f"[QA] User query: {user_query}")

        try:
            # Step 1: Format data
            print("[QA] Step 1: Formatting data...")
            format_type, formatted_data = choose_optimal_format(rows, "general_qa")
            data_block = (
                f"DATA ({'CSV' if format_type == 'csv' else 'JSON'}):\n"
                f"```{format_type}\n{formatted_data}\n```"
            )

            print(f"[QA] ✅ Data formatted as {format_type.upper()} ({len(formatted_data)} chars)")

            # Step 2: Build QA prompt
            prompt = f"""You are a data analysis assistant helping a user understand their invoice data.

    CONTEXT:
    - Always be precise, factual, and concise.
    - Follow the user's intent exactly.
    - Base all answers strictly on the provided data.

    {data_block}

    USER QUESTION: "{user_query}"

    STRICT INSTRUCTIONS:

    1. Treat each distinct value as a completely different category.
    2. Never group "System Accepted" with "Accepted".
    3. Never group "System Disputed" with "Disputed".
    4. Do NOT use substring matching for status fields; always match full string values.
    5. Invoice statuses and approval statuses must be treated as exact values.
    6. "highest" / "most expensive" → invoice(s) with MAX grand_total.
    7. "lowest" / "cheapest" → invoice(s) with MIN grand_total.
    8. "top N" → exactly N invoices; if ties at Nth position, include all tied.
    9. "least N" → exactly N invoices; if ties at Nth position, include all tied.
    10. NEVER infer missing data or assume values.
    11. Output format (one line per result):
        Invoice Date | Account | Provider | Site | Grand Total
    12. Return ONLY the answer lines.
    13. NO explanations, NO markdown, NO headings, NO extra text.

    Your answer:"""

            # Step 3: Call model
            print("[QA] Step 2: Generating answer...")

            def generate():
                text = self.model_service.generate_text(prompt)
                if not text or not text.strip():
                    raise RuntimeError("Empty response from model")
                return text.strip()

            result_text = retry_with_backoff(generate)

            if not result_text:
                return "⚠️ No answer could be generated from the data."

            print(f"[QA] ✅ Answer generated successfully ({len(result_text)} chars)")
            return result_text

        except Exception as e:
            print(f"[QA] ❌ Error: {e}")
            import traceback
            traceback.print_exc()
            return f"⚠️ Error answering the question: {str(e)}"
         
    def generate_forecast_chart(self, user_query: str, forecast_rows: List[Dict], size: str = "960x560") -> Optional[Dict]:
        """Generate a clean forecast line chart matching the style of generate_chart."""
        
        if not self.enabled:
            return None

        if not forecast_rows:
            return None

        try:
            # Format data
            format_type, formatted_data = choose_optimal_format(forecast_rows, "chart")
            data_block = f"DATA ({'CSV' if format_type=='csv' else 'JSON'}):\n```{format_type}\n{formatted_data}\n```"
            
            print(f"[FORECAST_CHART] Data formatted as {format_type.upper()} ({len(formatted_data)} chars)")
            print(f"csv conveted data for forecast chart : {data_block}")
            
            # Simplified, cleaner prompt focusing on straight lines and clarity
            prompt = f"""You are an expert data visualization specialist. Create a CLEAN, PROFESSIONAL forecast line chart.

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

            def generate_svg():
                text = self.model_service.generate_text(prompt)
                if not text or not text.strip():
                    raise RuntimeError("Empty SVG response from model")
                return text.strip()

            # Generate with retry
            raw = retry_with_backoff(generate_svg, max_retries=3, initial_delay=2)

            if not raw:
                print("[FORECAST_CHART] ⚠️ Empty response from model")
                return {
                    "image_b64": None,
                    "image_mime": None,
                    "error": "⚠️ Chart generation returned no content."
                }

            # Clean response
            raw = raw.strip()
            raw = re.sub(r'^```[\w]*\s*', '', raw, flags=re.MULTILINE)
            raw = re.sub(r'\s*```\s*$', '', raw, flags=re.MULTILINE)
            raw = raw.strip()

            # Extract SVG
            svg_start = re.search(r'<svg[\s>]', raw, re.IGNORECASE)
            svg_end = re.search(r'</svg\s*>', raw, re.IGNORECASE)
            
            if not svg_start or not svg_end:
                print(f"[FORECAST_CHART] ❌ No valid SVG tags found")
                print(f"[FORECAST_CHART] Response preview: {raw[:500]}")
                return {
                    "image_b64": None,
                    "image_mime": None,
                    "error": "⚠️ Invalid SVG generated."
                }

            svg = raw[svg_start.start():svg_end.end()].strip()

            # Validate
            if len(svg) < 100 or not svg.lower().startswith('<svg'):
                print(f"[FORECAST_CHART] ❌ Invalid SVG (length: {len(svg)})")
                return {
                    "image_b64": None,
                    "image_mime": None,
                    "error": "⚠️ Malformed SVG output."
                }

            # Encode
            b64 = base64.b64encode(svg.encode("utf-8")).decode("ascii")
            print(f"[FORECAST_CHART] ✅ Chart generated ({len(svg)} bytes)")
            
            return {
                "image_b64": b64,
                "image_mime": "image/svg+xml",
            }
            
        except Exception as e:
            print(f"[FORECAST_CHART] ❌ Error: {e}")
            import traceback
            traceback.print_exc()
            return {
                "image_b64": None,
                "image_mime": None,
                "error": f"⚠️ Chart generation error: {str(e)}"
            }
            
            
            
    

    def generate_summary(self, user_query: str, rows: List[Dict]) -> str:
        """Generate executive summary report using Gemini with intelligent aggregation."""
        if not self.enabled:
            print("[SUMMARY] Gemini disabled")
            return "⚠️ AI summary generation is currently disabled."

        if not rows:
            print("[SUMMARY] No rows")
            return "⚠️ No data available for summary."

        print(f"[SUMMARY] 📊 Starting summary generation with {len(rows)} rows")
        print(f"[SUMMARY] User query: {user_query}")

        try:
            # Step 1: Generate aggregation spec
            print("[SUMMARY] Step 1: Generating aggregation spec...")
            # spec_gen = SummarySpecGenerator(self.model_service)
            # available_columns = list(rows[0].keys())
            # sample_rows = rows[:3]
            
            # spec = spec_gen.get_default_spec(
            #     user_query=user_query,
            #     available_columns=available_columns,
            #     field_types=FIELD_TYPES,
            #     sample_rows=sample_rows
            # )
            
            
            spec = get_summary_spec()
            
            print(f"[SUMMARY] ✅ Spec generated with {len(spec.get('aggregations', []))} categories")
            print(f"[SUMMARY] Categories: {spec.get('include_categories', [])}")
            print(f"Summary Data Spec : {spec}")
            
            

            # Step 2: Aggregate the data
            print(f"[SUMMARY] Step 2: Aggregating {len(rows)} rows...")
            aggregated_summary = aggregate_for_summary(
                rows=rows,
                spec=spec,
                field_types=FIELD_TYPES
            )
            
            if "error" in aggregated_summary and aggregated_summary.get("total_records", 0) == 0:
                return f"⚠️ Aggregation error: {aggregated_summary['error']}"
        
            total_records = aggregated_summary.get("total_records", 0)
            print(f"[SUMMARY] ✅ Aggregated {total_records} records")
            print(f"[SUMMARY] Aggregation categories: {list(aggregated_summary.get('aggregations', {}).keys())}")
            

            # Step 3: Format aggregated summary for model
            print("[SUMMARY] Step 3: Formatting aggregated data...")
            summary_json = json.dumps(aggregated_summary, indent=2)
            data_block = f"AGGREGATED SUMMARY DATA (JSON):\n```json\n{summary_json}\n```"
            print(f" aggregated summary data : {summary_json}")
            
            print(f"[SUMMARY] ✅ Data formatted ({len(summary_json)} chars vs {len(str(rows))} original)")

            # Step 4: Extract metadata
            num_records = aggregated_summary.get("total_records", len(rows))
            
            # Get date range from aggregations
            date_info = ""
            time_agg = aggregated_summary.get("aggregations", {}).get("time", {})
            if isinstance(time_agg, dict) and "date_range" in time_agg:
                date_range = time_agg["date_range"]
                if date_range.get("min_date") and date_range.get("max_date"):
                    date_info = f"\n- Date Range: {date_range['min_date']} to {date_range['max_date']}"

            print(f"[SUMMARY] Records: {num_records}")
            if date_info:
                print(f"[SUMMARY] {date_info.strip()}")

            # Step 5: Build summary prompt with aggregated data
            prompt = f"""You are a senior business intelligence analyst preparing an executive summary report for management.

CONTEXT:
- Dataset: Invoice / Billing / Transaction Records
- Total Records: {total_records:,} invoices{date_info}
- Data Type: Pre-aggregated summary with detailed status breakdowns
- Always be clear, structured, and professional

USER REQUEST: "{user_query or 'Provide a comprehensive business summary and insights from this dataset.'}"

{data_block}

ANALYSIS REQUIREMENTS:

ABSOLUTE OUTPUT RULES:
- DO NOT include any title, introduction, greeting, explanation, preface, or metadata
  (e.g., no "Of course", no To/From/Date/Subject, no *** separators)

Generate a professional business report aimed at finance and operations leaders with the following sections:

## 1. Executive Summary (2–4 sentences)
- Briefly describe the dataset scope using aggregations.time.date_range
- Highlight the single most important business finding from the aggregated data
- Reference key metrics from aggregations.financial (total_billed, total_invoices)

## 2. Status & Risk Overview
**CRITICAL: Use the enhanced risk breakdown structure**

A) **Disputed Invoices Analysis**
- Use aggregations.risk.disputed_breakdown
- Show the summary: total_count, total_amount, percentage
- Analyze the breakdown table showing:
  - InvoiceStatusType (Disputed vs System Disputed)
  - InvoiceApprovalStatus (what stage they're stuck at)
  - PaymentStatus (payment readiness)
  - VerificationResult (verification status)
- Identify the most common combination causing issues
- Quantify the value impact of disputes

B) **Accepted Invoices Analysis**
- Use aggregations.risk.accepted_breakdown
- Show the summary: total_count, total_amount, percentage
- Analyze the breakdown table showing:
  - InvoiceStatusType (Accepted vs System Accepted)
  - InvoiceApprovalStatus (approval progress)
  - PaymentStatus (payment status)
  - VerificationResult (verification completion)
- Identify how many are fully processed (Verified + Approval Completed + Settled)
- Note any accepted invoices with incomplete processing

C) **Other Risk Indicators**
- Use aggregations.risk.not_verified (if present)
  - Count, total_amount, percentage of unverified invoices
- Use aggregations.risk.pending_approval (if present)
  - Count, total_amount, percentage of pending approvals

D) **Overall Risk Assessment**
- Calculate total at-risk value (disputed + not verified + pending)
- Comment on operational risk implications
- Identify process bottlenecks from the detailed breakdowns

## 3. Provider & Service Analysis
- Use aggregations.provider to identify top providers by total_amount
- Calculate vendor dependency (top provider's share of aggregations.financial.total_billed)
- If aggregations.service exists, summarize main service types
- Comment on concentration vs. diversification

## 4. Cost Center & Location View
- Use aggregations.cost_center to list top locations by total_amount
- Calculate spending concentration (top 3 share of total)
- Cross-reference with risk data if location-specific patterns exist
- Note any concentration issues

## 5. Financial Snapshot
Use aggregations.financial for all metrics:
- **Total Billed Value**: total_billed
- **Average Invoice Value**: average_invoice
- **Invoice Range**: min_invoice to max_invoice
- **Median Invoice**: median_invoice
- **Total Tax**: total_tax (if available)
- **Net Total**: total_net (if available)
- **Total Usage**: total_usage (if available)
- **Total Rental**: total_rental (if available)
Interpret what these numbers mean for spending patterns

## 6. Operational & Process Insights
- Analyze aggregations.risk.accepted_breakdown and aggregations.risk.disputed_breakdown together
- Calculate processing efficiency:
  - % of invoices fully processed (Verified + Approval Completed)
  - % stuck in approval workflow
  - % with verification issues
- Identify bottlenecks in approval/verification process from breakdown patterns
- Note any patterns where specific status combinations are common

## 7. Risk Factors & Red Flags
- Use aggregations.risk.disputed_breakdown.summary for total disputed exposure
- Highlight most problematic status combinations from the breakdowns
- Use aggregations.status to identify unusual patterns
- Call out high-value outliers in aggregations.provider or aggregations.cost_center
- Note data quality issues if evident

## 8. Key Recommendations (3–6 bullet points)
- Provide concrete, action-oriented recommendations
- Prioritize based on aggregations.risk data (focus on highest-value issues)
- Reference specific status combinations that need attention
- Suggest process improvements based on breakdown patterns
- Include cost optimization opportunities from provider/cost_center data
- Recommend data quality fixes if needed

FORMATTING RULES:
- Use markdown headers (##) for sections exactly as above
- Use **bold** for key metrics and numbers
- Always include specific numbers and percentages
- Format large numbers with commas (e.g., "1,234,567")
- Do NOT invent currency symbols; use numeric values only unless currency is explicit
- Keep each section concise (3–5 sentences or bullet points)
- Total length: roughly 500–700 words
- Maintain formal, professional business tone

CRITICAL RULES FOR RISK SECTION:
- aggregations.risk.disputed_breakdown has TWO parts:
  1. summary: {{"total_count": X, "total_amount": Y, "percentage": Z}}
  2. breakdown: [{{"InvoiceStatusType": "...", "InvoiceApprovalStatus": "...", ...}}]
- aggregations.risk.accepted_breakdown has the SAME structure
- ALWAYS analyze BOTH the summary AND the breakdown table
- Compare patterns between disputed and accepted invoices
- Base ALL statements strictly on the aggregated data provided
- NEVER make assumptions or add information not in the aggregations
- If data is missing for a section, state: "Insufficient data for [section name] analysis."
- Always compare and contrast (e.g., "Disputed: 25% vs Accepted: 75%")  

    Generate the comprehensive report now:"""

            # Step 6: Call model and return result
            print("[SUMMARY] Step 4: Generating summary report from aggregated data...")
            
            def generate():
                text = self.model_service.generate_text(prompt)
                if not text or not text.strip():
                    raise RuntimeError("Empty response from model")
                return text.strip()

            result_text = retry_with_backoff(generate)
            
            if not result_text:
                return "⚠️ No summary could be generated from the data."

            print(f"[SUMMARY] ✅ Summary generated successfully ({len(result_text)} chars)")
            print(f"[SUMMARY] 📊 Data reduction: {len(str(rows))} chars → {len(summary_json)} chars ({round(len(summary_json)/len(str(rows))*100, 1)}%)")
            
            return result_text

        except Exception as e:
            print(f"[SUMMARY] ❌ Error: {e}")
            import traceback
            traceback.print_exc()
            return f"⚠️ Error generating summary: {str(e)}"
