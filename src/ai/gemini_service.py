import os
import re
import json
import base64
import csv
from io import StringIO
from typing import Dict, List, Optional, Tuple
import time
from src.ai.chart_spec_generator import SpecGenerator
from src.aggregator.chart_aggregator import aggregate_rows
from src.utils.utils import retry_with_backoff,choose_optimal_format,restore_original_columns,get_summary_spec
from src.models.model_service import ModelService
from src.ai.forecast_spec_generator import ForecastSpecGenerator
from src.aggregator.forecast_aggregator import prepare_forecast_data,validate_and_fix_forecast_spec
from src.ai.summary_spec_generator import SummarySpecGenerator
from src.aggregator.summary_aggregator import aggregate_for_summary
from src.config.field_constant import TABLE_SCHEMAS
            







class GeminiService:
    
    PURPOSE = "Summary"
    
    def __init__(self, model_service: ModelService):
        self.model_service = model_service
        self.enabled = model_service.has_purpose(self.PURPOSE)
    
 
    def generate_summary_1(self, user_query: str, rows: List[Dict]) -> str:
        """Generate executive summary report using Gemini for ERP data."""
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

            # Detect date range from common date columns
            date_info = ""
            date_columns = ["Date Received", "Inspected On", "REC_DATE", "Transaction Date", 
                        "COST_AS_OF_DATE", "Alpha Date Received"]
            
            for date_col in date_columns:
                if date_col in sample_cols:
                    try:
                        dates = [r.get(date_col) for r in rows if r.get(date_col) and r.get(date_col) != ' ']
                        if dates:
                            date_info = f"\n- Date Range: {min(dates)} to {max(dates)}"
                            break
                    except:
                        pass

            print(f"[SUMMARY] Records: {num_records}, Columns: {len(sample_cols)}")
            if date_info:
                print(f"[SUMMARY] {date_info.strip()}")

            # Step 3: Detect data type from columns
            data_type = "General ERP Data"
            if "Inspection Item" in sample_cols or "Rejected Quantity" in sample_cols:
                data_type = "Inspection / Quality Control Records"
            elif "RECEIVER" in sample_cols or "REC_DATE" in sample_cols:
                data_type = "Receipt / Purchase Order Records"
            elif "Transaction Date" in sample_cols or "Source Type" in sample_cols:
                data_type = "Inventory Transaction Records"
            elif "ITEM_DETAIL_ON_HAND_QUANTITY" in sample_cols or "LOCATION_NAME" in sample_cols:
                data_type = "Inventory On-Hand Records"

            # Step 4: Build ERP summary prompt
            prompt = f"""You are a senior ERP analyst and operations manager preparing an executive summary report for management.

    CONTEXT:
    - Dataset: {data_type}
    - Total Records: {num_records} records{date_info}
    - Company/Tenant: Manufacturing/Distribution ERP System
    - Always be clear, structured, and professional

    USER REQUEST: "{user_query or 'Provide a comprehensive operational summary and insights from this dataset.'}"

    {data_block}

    ANALYSIS REQUIREMENTS:

    ABSOLUTE OUTPUT RULES:
    - DO NOT include any title, introduction, greeting, explanation, preface, or metadata
    (e.g., no "Of course", no To/From/Date/Subject, no *** separators)
    - Start directly with "## 1. Executive Summary"

    Generate a professional operations report aimed at supply chain, quality, and operations leaders with the following sections:

    ## 1. Executive Summary (2–4 sentences)
    - Briefly describe the dataset scope (time period, record count, data type)
    - Highlight the single most important operational finding
    - Reference key metrics (quantities, values, counts)
    - Set context for decision-makers

    ## 2. Quality & Inspection Analysis (if applicable)
    **For Inspection Data:**
    - Total inspections performed and date range
    - Acceptance vs. Rejection rates (calculate percentages)
    - Top items with highest rejection quantities
    - Inspection reason analysis (most common failure reasons)
    - Quality trends (improving/declining over time period)
    - Vendor quality performance (if vendor data present)
    - Risk assessment: High-value items with quality issues

    **Skip this section if not inspection data**

    ## 3. Inventory Movement & Transactions (if applicable)
    **For Transaction Data:**
    - Total transaction volume and types (receipts, issues, adjustments, transfers)
    - Most active items by transaction count
    - Transaction patterns by source type
    - Inventory velocity analysis (fast vs slow moving items)
    - Location-based transaction patterns
    - Unusual transaction patterns or outliers

    **Skip this section if not transaction data**

    ## 4. Receipt & Procurement Analysis (if applicable)
    **For Receipt Data:**
    - Total receipts processed and value
    - Top vendors by receipt volume and value
    - Average receipt size and quantity patterns
    - Receipt timing patterns (delivery performance)
    - Items most frequently received
    - Purchase order completion rates

    **Skip this section if not receipt data**

    ## 5. Inventory Status & Stock Levels (if applicable)
    **For On-Hand Inventory:**
    - Total inventory value across all locations
    - Inventory distribution by location/warehouse
    - High-value inventory items and concentration
    - Stock level analysis (adequate/overstocked/understocked)
    - Slow-moving or obsolete inventory identification
    - Location utilization and capacity

    **Skip this section if not inventory on-hand data**

    ## 6. Item & Product Analysis
    - Top items by transaction volume, value, or quantity
    - Item classification (A/B/C analysis if applicable)
    - Product line or commodity analysis
    - High-impact items requiring attention
    - Item diversity vs. concentration
    - Critical items identification

    ## 7. Operational Insights & Patterns
    - Time-based patterns (monthly, seasonal trends)
    - Process efficiency indicators
    - Bottlenecks or delays identified
    - Data quality observations
    - Cross-functional patterns (quality vs. volume, cost vs. velocity)
    - Unexpected findings or anomalies
    - Performance metrics summary

    ## 8. Risk Factors & Red Flags
    - Quality risks (high rejection rates, critical item failures)
    - Inventory risks (stockouts, excess inventory, obsolescence)
    - Vendor risks (poor quality, delivery issues)
    - Process risks (delays, inefficiencies, errors)
    - Financial risks (high-value at-risk inventory)
    - Operational risks (capacity constraints, single-source dependencies)
    - Data quality issues requiring attention

    ## 9. Key Recommendations (4–6 bullet points)
    - Provide concrete, action-oriented recommendations
    - Prioritize based on impact and urgency
    - Quality improvement actions (if quality issues found)
    - Inventory optimization opportunities
    - Process improvement suggestions
    - Vendor management recommendations
    - Cost reduction opportunities
    - Data quality improvements needed

    FORMATTING RULES:
    - Use markdown headers (##) for sections exactly as above
    - Use **bold** for key metrics and numbers
    - Always include specific numbers and percentages
    - Format large numbers with commas (e.g., "1,234.56")
    - Use bullet points for lists and multiple items
    - Keep each section concise (3–6 sentences or bullet points)
    - Total length: roughly 600–800 words
    - Maintain formal, professional business tone
    - Skip sections that don't apply to the data type

    CRITICAL ANALYSIS RULES:
    - Base ALL statements strictly on the provided data
    - Calculate percentages, totals, and aggregations from the data
    - NEVER make assumptions or add information not in the data
    - If data is missing for a section, state: "Insufficient data for [section name] analysis."
    - Always quantify findings with specific numbers
    - Compare and contrast patterns (e.g., "Location A: 45% vs Location B: 30%")
    - Identify outliers and explain their significance
    - Connect insights to business impact

    DATA-SPECIFIC GUIDANCE:

    **For Inspection Data - Look For:**
    - Rejected Quantity vs Accepted Quantity patterns
    - Items with consistent quality issues
    - Inspection reasons (defects, damage, specification failures)
    - Vendor quality performance trends
    - Cost impact of rejections

    **For Transaction Data - Look For:**
    - Transaction Date patterns and frequency
    - Source Type distribution (issues, receipts, adjustments, transfers)
    - Item Number transaction volumes
    - Quantity patterns (positive/negative)
    - Location-based activity levels

    **For Receipt Data - Look For:**
    - REC_DATE patterns and timing
    - VENDOR performance and volume
    - ITEM receipt frequencies
    - BUY_QTY patterns and order sizes
    - RECEIVER workload distribution

    **For Inventory Data - Look For:**
    - ITEM_DETAIL_ON_HAND_QUANTITY levels
    - LOCATION_NAME distribution
    - EXTENDED_AMOUNT (total value) concentration
    - Item Type classification
    - Stock adequacy vs demand patterns

    Generate the comprehensive ERP operational report now:"""

            # Step 5: Call model and return result
            print("[SUMMARY] Step 2: Generating ERP summary report...")
            
            def generate():
                text = self.model_service.generate(self.PURPOSE, prompt).strip()
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
        
    
    def generate_summary(self, user_query: str, rows: List[Dict]) -> str:
        """Generate executive summary report using Gemini for ERP data."""
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

            # Detect date range from common date columns
            date_info = ""
            date_columns = ["Date Received", "Inspected On", "REC_DATE", "Transaction Date", 
                        "COST_AS_OF_DATE", "Alpha Date Received"]
            
            for date_col in date_columns:
                if date_col in sample_cols:
                    try:
                        dates = [r.get(date_col) for r in rows if r.get(date_col) and r.get(date_col) != ' ']
                        if dates:
                            date_info = f"\n- Date Range: {min(dates)} to {max(dates)}"
                            break
                    except:
                        pass

            print(f"[SUMMARY] Records: {num_records}, Columns: {len(sample_cols)}")
            if date_info:
                print(f"[SUMMARY] {date_info.strip()}")

            # Step 3: Detect data type from columns
            data_type = "General ERP Data"
            if "Inspection Item" in sample_cols or "Rejected Quantity" in sample_cols:
                data_type = "Inspection / Quality Control Records"
            elif "RECEIVER" in sample_cols or "REC_DATE" in sample_cols:
                data_type = "Receipt / Purchase Order Records"
            elif "Transaction Date" in sample_cols or "Source Type" in sample_cols:
                data_type = "Inventory Transaction Records"
            elif "ITEM_DETAIL_ON_HAND_QUANTITY" in sample_cols or "LOCATION_NAME" in sample_cols:
                data_type = "Inventory On-Hand Records"

            # Step 4: Build ERP summary prompt
            prompt = f"""You are a senior ERP analyst and operations manager preparing an executive summary report for management.

CONTEXT:
- Dataset: {data_type}
- Total Records: {num_records} records{date_info}
- Company/Tenant: Manufacturing/Distribution ERP System
- Always be clear, structured, and professional

USER REQUEST: "{user_query or 'Provide a comprehensive operational summary and insights from this dataset.'}"

{data_block}

==================================================================================
STEP 0 — INTENT PARSING (complete this BEFORE writing any output):
==================================================================================
Read USER REQUEST carefully and extract the following signals:

SIGNAL A — SECTION FOCUS
Scan for keywords that map to specific report sections:
  "executive summary", "overview"           → Section 1
  "quality", "inspection", "rejection"      → Section 2
  "transaction", "movement", "transfer"     → Section 3
  "receipt", "procurement", "purchase"      → Section 4
  "inventory", "stock", "on-hand", "on hand"→ Section 5
  "item", "product", "sku"                  → Section 6
  "insight", "pattern", "trend", "operational" → Section 7
  "risk", "red flag", "issue", "problem"    → Section 8
  "recommendation", "action", "suggest"     → Section 9

SIGNAL B — ENTITY FOCUS
Check if a specific vendor name, item number, or location is mentioned.
  Example: "for vendor ABC", "item XYZ-001", "at warehouse 02"
  If found → scope ALL analysis strictly to that entity. Ignore others in data.
  If not found → analyse all entities in the dataset.

SIGNAL C — AUDIENCE / TONE
Check for audience keywords:
  "finance", "financial", "CFO", "cost"     → Focus on values, costs, financial impact
  "executive", "management", "CEO", "board" → High-level, concise, strategic language
  "operations", "warehouse", "supply chain" → Operational detail, process focus
  If none found → use default professional operations tone.

DECISION RULE (strict):
  - If SIGNAL A found specific sections → generate ONLY those sections. Nothing else.
  - If SIGNAL A not found → generate ALL applicable sections (full default report).
  - If SIGNAL B found → filter all analysis to that entity only.
  - If SIGNAL C found → apply that tone throughout the entire output.
  - Sections that don't apply to the data type are always skipped regardless.

DO NOT output the intent parsing. Start output directly with the first relevant section.
==================================================================================

ABSOLUTE OUTPUT RULES:
- DO NOT include any title, introduction, greeting, explanation, preface, or metadata
  (e.g., no "Of course", no To/From/Date/Subject, no *** separators)
- Start directly with the first relevant ## section header
- Generate ONLY the sections determined by STEP 0 intent parsing above

Generate a professional operations report aimed at supply chain, quality, and operations leaders with the following sections:

##  Executive Summary (2–4 sentences)
- Briefly describe the dataset scope (time period, record count, data type)
- Highlight the single most important operational finding
- Reference key metrics (quantities, values, counts)
- Set context for decision-makers

##  Quality & Inspection Analysis (if applicable)
**For Inspection Data:**
- Total inspections performed and date range
- Acceptance vs. Rejection rates (calculate percentages)
- Top items with highest rejection quantities
- Inspection reason analysis (most common failure reasons)
- Quality trends (improving/declining over time period)
- Vendor quality performance (if vendor data present)
- Risk assessment: High-value items with quality issues

**Skip this section if not inspection data**

##  Inventory Movement & Transactions (if applicable)
**For Transaction Data:**
- Total transaction volume and types (receipts, issues, adjustments, transfers)
- Most active items by transaction count
- Transaction patterns by source type
- Inventory velocity analysis (fast vs slow moving items)
- Location-based transaction patterns
- Unusual transaction patterns or outliers

**Skip this section if not transaction data**

##  Receipt & Procurement Analysis (if applicable)
**For Receipt Data:**
- Total receipts processed and value
- Top vendors by receipt volume and value
- Average receipt size and quantity patterns
- Receipt timing patterns (delivery performance)
- Items most frequently received
- Purchase order completion rates

**Skip this section if not receipt data**

##  Inventory Status & Stock Levels (if applicable)
**For On-Hand Inventory:**
- Total inventory value across all locations
- Inventory distribution by location/warehouse
- High-value inventory items and concentration
- Stock level analysis (adequate/overstocked/understocked)
- Slow-moving or obsolete inventory identification
- Location utilization and capacity

**Skip this section if not inventory on-hand data**

##  Item & Product Analysis
- Top items by transaction volume, value, or quantity
- Item classification (A/B/C analysis if applicable)
- Product line or commodity analysis
- High-impact items requiring attention
- Item diversity vs. concentration
- Critical items identification

##  Operational Insights & Patterns
- Time-based patterns (monthly, seasonal trends)
- Process efficiency indicators
- Bottlenecks or delays identified
- Data quality observations
- Cross-functional patterns (quality vs. volume, cost vs. velocity)
- Unexpected findings or anomalies
- Performance metrics summary

##  Risk Factors & Red Flags
- Quality risks (high rejection rates, critical item failures)
- Inventory risks (stockouts, excess inventory, obsolescence)
- Vendor risks (poor quality, delivery issues)
- Process risks (delays, inefficiencies, errors)
- Financial risks (high-value at-risk inventory)
- Operational risks (capacity constraints, single-source dependencies)
- Data quality issues requiring attention

##  Key Recommendations (4–6 bullet points)
- Provide concrete, action-oriented recommendations
- Prioritize based on impact and urgency
- Quality improvement actions (if quality issues found)
- Inventory optimization opportunities
- Process improvement suggestions
- Vendor management recommendations
- Cost reduction opportunities
- Data quality improvements needed

FORMATTING RULES:
- Use markdown headers (##) for sections exactly as above
- Use **bold** for key metrics and numbers
- Always include specific numbers and percentages
- Format large numbers with commas (e.g., "1,234.56")
- Use bullet points for lists and multiple items
- Keep each section concise (3–6 sentences or bullet points)
- Total length: roughly 600–800 words for full report; proportionally shorter for focused requests
- Maintain the tone determined by SIGNAL C from STEP 0
- Skip sections that don't apply to the data type
- Skip sections not requested by the user (per STEP 0 DECISION RULE)

CRITICAL ANALYSIS RULES:
- Base ALL statements strictly on the provided data
- Calculate percentages, totals, and aggregations from the data
- NEVER make assumptions or add information not in the data
- If data is missing for a section, state: "Insufficient data for [section name] analysis."
- Always quantify findings with specific numbers
- Compare and contrast patterns (e.g., "Location A: 45% vs Location B: 30%")
- Identify outliers and explain their significance
- Connect insights to business impact
- If SIGNAL B (entity focus) was detected, restrict ALL data points to that entity only

DATA-SPECIFIC GUIDANCE:

**For Inspection Data - Look For:**
- Rejected Quantity vs Accepted Quantity patterns
- Items with consistent quality issues
- Inspection reasons (defects, damage, specification failures)
- Vendor quality performance trends
- Cost impact of rejections

**For Transaction Data - Look For:**
- Transaction Date patterns and frequency
- Source Type distribution (issues, receipts, adjustments, transfers)
- Item Number transaction volumes
- Quantity patterns (positive/negative)
- Location-based activity levels

**For Receipt Data - Look For:**
- REC_DATE patterns and timing
- VENDOR performance and volume
- ITEM receipt frequencies
- BUY_QTY patterns and order sizes
- RECEIVER workload distribution

**For Inventory Data - Look For:**
- ITEM_DETAIL_ON_HAND_QUANTITY levels
- LOCATION_NAME distribution
- EXTENDED_AMOUNT (total value) concentration
- Item Type classification
- Stock adequacy vs demand patterns

Generate the report now:"""

            # Step 5: Call model and return result
            print("[SUMMARY] Step 2: Generating ERP summary report...")
            
            def generate():
                text = self.model_service.generate(self.PURPOSE, prompt).strip()
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
            # Step 1: Infer field types from schema
            
            available_columns = list(rows[0].keys())
            sample_rows = rows[:3]
            
            # Build field type mapping from schema
            field_types = {}
            field_descriptions = {}
            
            for table_name, table_schema in TABLE_SCHEMAS.items():
                for col_name, col_type in table_schema["columns"].items():
                    if col_name in available_columns:
                        # Map to simple types
                        if "DATE" in col_type.upper():
                            field_types[col_name] = "date"
                        elif "NUMBER" in col_type.upper() or "NUMERIC" in col_type.upper():
                            field_types[col_name] = "number"
                        elif "VARCHAR" in col_type.upper() or "CHAR" in col_type.upper():
                            field_types[col_name] = "string"
                        else:
                            field_types[col_name] = "string"
                        
                        field_descriptions[col_name] = col_type
            
            # Fallback for columns not in schema
            for col in available_columns:
                if col not in field_types:
                    sample_val = rows[0].get(col) if rows else None
                    if isinstance(sample_val, (int, float)):
                        field_types[col] = "number"
                    elif isinstance(sample_val, str):
                        if re.match(r'\d{4}-\d{2}-\d{2}', str(sample_val)):
                            field_types[col] = "date"
                        else:
                            field_types[col] = "string"
                    else:
                        field_types[col] = "string"
            
            print(f"[FORECAST] Inferred field types: {field_types}")
            
            # Step 2: Generate forecast spec
            print("[FORECAST] Step 2: Generating forecast spec...")
            spec_gen = ForecastSpecGenerator(self.model_service)
            
            spec = spec_gen.generate_spec(
                user_query=user_query,
                available_columns=available_columns,
                field_types=field_types,
                sample_rows=sample_rows
            )
            
            print(f"[FORECAST] ✅ Spec generated: {json.dumps(spec, indent=2)}")

            # Step 3: Prepare forecast data using aggregator
            print(f"[FORECAST] Step 3: Preparing {len(rows)} rows for forecasting...")
            forecast_data, validated_spec = prepare_forecast_data(
                rows=rows,
                spec=spec,
                field_types=field_types,
                max_groups=50
            )
            
            print(f"[FORECAST] ✅ Prepared {len(forecast_data)} records")
            print(f"[FORECAST] ✅ Validated spec: {json.dumps(validated_spec, indent=2)}")
            
            if not forecast_data:
                return "⚠️ No valid data for forecasting. Please check your date and value columns."

            # Step 4: Format prepared data for model
            print("[FORECAST] Step 4: Formatting data...")
            format_type, formatted_data = choose_optimal_format(forecast_data, "forecast")
            data_block = f"DATA ({'CSV' if format_type=='csv' else 'JSON'}):\n```{format_type}\n{formatted_data}\n```"
            print(f"[FORECAST] Data for forecast: {data_block[:500]}...")
            
            # Restore original columns for chart
            chart_data = restore_original_columns(forecast_data, validated_spec)
            
            print(f"[FORECAST] ✅ Data formatted as {format_type.upper()} ({len(formatted_data)} chars)")

            # Step 5: Extract info from validated spec for prompt
            date_column = validated_spec.get("date_column", "date")
            value_columns = validated_spec.get("value_columns", [])
            grouping_columns = validated_spec.get("grouping_columns", [])
            time_bucket = validated_spec.get("time_bucket", "month")
            forecast_periods = validated_spec.get("forecast_periods", periods)
            has_groups = "group" in (forecast_data[0].keys() if forecast_data else [])
            
            print(f"[FORECAST] Forecasting: {value_columns} by {time_bucket}")
            if has_groups:
                print(f"[FORECAST] With grouping by: {grouping_columns}")

            # Step 6: Build field context
            field_context = "\n".join([
                f"- {col}: {field_types.get(col, 'unknown')} ({field_descriptions.get(col, 'derived')})"
                for col in available_columns[:10]  # Limit to first 10 for brevity
            ])

            # Step 7: Identify domain context
            domain_context = "purchasing/procurement"
            if any("supplier" in col.lower() for col in available_columns):
                domain_context = "supplier spend and purchasing"
            elif any("order" in col.lower() for col in available_columns):
                domain_context = "purchase order volume"
            
            # Step 8: Build forecast prompt (markdown output, not JSON)
            prompt = f"""You are a senior data analyst specializing in {domain_context} forecasting.

    DATA SCHEMA:
    {field_context}

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
    - NO "Of course", NO To/From/Date/Subject, NO *** separators
    - Start directly with "## Forecast Summary"

    FORECAST REQUIREMENTS:

    1. Data Understanding
    - Analyze the time series pattern (trend, seasonality, volatility)
    - Identify any anomalies or outliers in historical data
    - Comment on data quality and completeness

    2. Forecast Generation
    - Generate {forecast_periods} future {time_bucket} predictions for each value column
    - Use appropriate forecasting method based on the pattern (trend-based, seasonal, or simple moving average)
    - {'Generate separate forecasts for each group' if has_groups else 'Generate single unified forecast'}
    - Consider domain-specific factors ({domain_context} patterns)

    3. Business Insights
    - Explain the trend direction and magnitude
    - Highlight potential risks or opportunities
    - Provide confidence assessment
    - Relate insights to business operations

    CRITICAL OUTPUT FORMAT:
    Return a well-structured markdown report with:

    ## Forecast Summary
    Brief overview of predictions and key findings (2-3 sentences)

    ## Historical Analysis
    - Pattern observed: [trend description - increasing/decreasing/stable/seasonal]
    - Data quality: [complete/partial/sparse - assessment of data coverage]
    - Notable observations: [outliers, gaps, seasonal patterns, etc.]

    ## Predictions
    Present forecast in a clear table format:

    | Period | {' | '.join(value_columns)} | Confidence |
    |--------|{('--------|' * len(value_columns))}-----------|
    | {time_bucket} 1 | [values] | High/Medium/Low |
    | {time_bucket} 2 | [values] | High/Medium/Low |
    ...

    **Note:** 
    - Ensure periods follow chronological order starting from the next {time_bucket}
    - Format numbers with commas (e.g., 12,345.67)
    - Use appropriate units ($ for currency, units for quantity)
    - Show confidence based on historical volatility and pattern strength

    ## Key Insights
    - **Trend**: [increasing/decreasing/stable - describe the overall direction and rate]
    - **Seasonality**: [detected/not_detected - describe seasonal patterns if any, e.g., "monthly cycle with peaks in Q4"]
    - **Key Drivers**: [factor 1, factor 2, factor 3 - based on domain knowledge]
    - **Confidence Level**: [high/medium/low - overall confidence in predictions with reasoning]
    - **Volatility**: [low/medium/high - describe variability in historical data]
    - **Notes**: [Important observations about forecast assumptions or limitations]

    ## Recommendations
    1. [Action 1 - specific, actionable recommendation based on forecast]
    2. [Action 2 - e.g., "Plan for X% increase in budget"]
    3. [Action 3 - e.g., "Review supplier contracts before peak period"]
    4. [Action 4 - if applicable]

    ## Risk Factors
    - **Risk 1**: [Potential issue - e.g., "Seasonal spike may exceed 20% of baseline"]
    - **Risk 2**: [e.g., "Limited historical data reduces confidence in long-term predictions"]
    - **Risk 3**: [e.g., "External factors (market conditions, policy changes) not reflected in model"]

    FORMATTING RULES:
    - Use markdown headers (##) for sections
    - Use **bold** for key numbers and metrics
    - Include specific values and percentages
    - Keep professional business tone
    - Base ALL statements on the provided data
    - Confidence values:
    * "High" - clear pattern with <10% historical volatility
    * "Medium" - recognizable pattern with 10-25% volatility
    * "Low" - unstable pattern or >25% volatility
    - Date format: YYYY-MM-DD or "Jan 2025" format
    - Numbers: Use commas and appropriate precision (2 decimals for currency)
    - Total length: 400-700 words

    Generate the comprehensive forecast report now:"""

            # Step 9: Call model and return result
            print("[FORECAST] Step 9: Generating forecast predictions...")
            
            def generate():
                text = self.model_service.generate(self.PURPOSE, prompt).strip()
                if not text or not text.strip():
                    raise RuntimeError("Empty response from model")
                return text.strip()

            result_text = retry_with_backoff(generate, max_retries=3, initial_delay=2)
            
            if not result_text:
                return "⚠️ No forecast could be generated from the data."
            
            print(f"[FORECAST] ✅ Forecast generated successfully ({len(result_text)} chars)")
            
            return {
                "text": result_text,
                "forecast_rows": chart_data,
                "field_types": field_types,
                "validated_spec": validated_spec
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
            
            # Import schema for field type inference
            
            
            # Infer field types from schema
            field_types = {}
            for table_name, table_schema in TABLE_SCHEMAS.items():
                for col_name, col_type in table_schema["columns"].items():
                    if col_name in available_columns:
                        # Map Oracle/PostgreSQL types to simple categories
                        if "DATE" in col_type.upper():
                            field_types[col_name] = "date"
                        elif "NUMBER" in col_type.upper() or "NUMERIC" in col_type.upper():
                            field_types[col_name] = "number"
                        elif "VARCHAR" in col_type.upper() or "CHAR" in col_type.upper():
                            field_types[col_name] = "string"
                        else:
                            field_types[col_name] = "string"
            
            # Fallback for columns not in schema
            for col in available_columns:
                if col not in field_types:
                    # Infer from sample data
                    sample_val = sample_rows[0].get(col) if sample_rows else None
                    if isinstance(sample_val, (int, float)):
                        field_types[col] = "number"
                    elif isinstance(sample_val, str):
                        # Check if it looks like a date
                        if re.match(r'\d{4}-\d{2}-\d{2}', str(sample_val)):
                            field_types[col] = "date"
                        else:
                            field_types[col] = "string"
                    else:
                        field_types[col] = "string"
            
            print(f"[CHART] Inferred field types: {field_types}")
            
            spec = spec_gen.generate_spec(
                user_query=user_query,
                task="chart",
                available_columns=available_columns,
                field_types=field_types,
                sample_rows=sample_rows,
                chart_hint=None  # Let model decide based on query
            )
            
            print(f"[CHART] ✅ Spec generated: {json.dumps(spec, indent=2)}")

            # Step 2: Aggregate the data using pandas
            print(f"[CHART] Step 2: Aggregating {len(rows)} rows...")
            agg_rows = aggregate_rows(rows, spec, field_types, max_groups=100)
            print(f"[CHART] ✅ Aggregated to {len(agg_rows)} rows")
            
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
            
            print(f"[CHART] ✅ Data formatted as {format_type.upper()} ({len(formatted_data)} chars)")

            # Step 4: Detect chart type from spec
            chart_type = spec.get("chart_type", "bar")
            print(f"[CHART] Chart type: {chart_type}")

            # Step 5: Generate SVG chart
            print("[CHART] Step 5: Generating SVG visualization...")
            
            prompt = f"""You are an expert data visualization specialist. Your task is to analyze the user's request and data, then create the most appropriate chart type.

    # CHART TYPE SELECTION RULES:
    - Analyze the data structure and user query to determine the best visualization
    - Consider: time series → line/area chart, comparisons → bar/column chart, proportions → pie chart, correlations → scatter plot, distributions → histogram, hierarchies → treemap, financial data → candlestick, density → heatmap
    - Choose the chart type that best communicates the insight the user is seeking

    # CRITICAL REQUIREMENTS FOR ALL CHARTS:
    1. **Clean, Readable Axes:**
    - X-axis: Clear labels, proper spacing, readable font (12-14px)
    - Y-axis: Clean numeric values with appropriate formatting (e.g., 1K, 1M for large numbers)
    - Both axes must have visible tick marks and grid lines (subtle)
    - Axis titles should be descriptive and positioned clearly

    2. **Data Point Labels:**
    - For bar/column charts: Show values on top/end of bars
    - For line charts: Show values at data points (optional if too dense)
    - For pie charts: Show percentages and labels clearly
    - Use contrasting colors for readability against dark background

    3. **Formatting Standards:**
    - Numbers: Format with commas (e.g., 1,234,567)
    - Currency: Use $ symbol and appropriate decimals
    - Percentages: Show with % symbol
    - Dates: Use clear format (Jan 2024, Q1 2024, etc.)

    4. **Legend Requirements:**
    - Include legend when multiple series/categories exist
    - Position: top-right or bottom, never overlapping data
    - Use clear, descriptive labels
    - Match colors exactly to chart elements

    5. **Visual Design:**
    - Background: #0f172a (dark slate)
    - Text color: #ffffff (white) or #e2e8f0 (light gray) for readability
    - Grid lines: #334155 (subtle, semi-transparent)
    - Use high-contrast color palette for series:
        * Primary: #3b82f6 (blue)
        * Secondary: #10b981 (green)
        * Tertiary: #f59e0b (amber)
        * Additional: #ef4444 (red), #8b5cf6 (purple), #06b6d4 (cyan)
    - Ensure all elements are clearly visible on dark background

    6. **Chart-Specific Requirements:**

    **Line/Area Charts:**
    - Smooth curves preferred
    - Data points marked with circles
    - Clear trend lines
    - Shaded area for area charts (semi-transparent)
    - Multiple lines if comparing multiple metrics

    **Bar/Column Charts:**
    - Adequate spacing between bars
    - Values displayed on bars
    - Bars should be proportional to values

    **Pie Charts:**
    - Show both percentage and value
    - Labels with leader lines if needed
    - Start at 12 o'clock position
    - Ensure slices are proper wedge shapes (center-to-arc triangular wedge)

    **Scatter Plots:**
    - Point size proportional to third dimension if applicable
    - Include trend line if correlation exists
    - Clear axis ranges

    **Histograms:**
    - Equal bin widths
    - Show frequency/count
    - Clear bin labels

    **Other chart types:**
    - Follow appropriate visualization best practices for bubble, waterfall, bullet, treemap, funnel, candlestick, heatmap, and density charts

    7. **Accessibility:**
    - Minimum font size: 12px
    - High contrast ratios
    - No color-only encoding (use patterns/shapes too if critical)

    # OUTPUT RULES:
    - Output ONLY the SVG code
    - No markdown code fences (no ```)
    - No explanatory text before or after
    - Start directly with <svg and end with </svg>
    - SVG must be valid and self-contained
    - Use the specified size exactly

    # USER REQUEST: "{user_query}"

    {data_block}

    # CHART SPECIFICATIONS:
    - Canvas Size: {size}
    - Background Color: #0f172a
    - Ensure all axes are clearly labeled with titles
    - Format all numeric values appropriately (commas for thousands, K/M for large numbers)
    - Display data point values where appropriate
    - Include legend if multiple series exist
    - Use high-contrast colors for dark background
    - All text must be readable (minimum 12px)

    # TASK:
    1. Analyze the user request and data structure
    2. Select the most appropriate chart type (line, bar, column, pie, scatter, area, histogram, bubble, waterfall, bullet, treemap, funnel, candlestick, heatmap, density, or other)
    3. Create a clean, professional visualization with properly formatted axes and clear labels
    4. Output ONLY the SVG code with no additional text, explanations, or markdown formatting

    Generate the chart now:"""

            def generate_svg():
                text = self.model_service.generate(self.PURPOSE, prompt).strip()
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
                "data_points": len(agg_rows),
                "field_types": field_types  # Include for debugging
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
        """Answer ad-hoc analytical questions on purchasing data using Gemini."""
        
        if not self.enabled:
            print("[QA] Gemini disabled")
            return "⚠️ AI analysis is currently disabled."

        if not rows:
            print("[QA] No rows")
            return "⚠️ No data available to answer the question."

        print(f"[QA] ❓ Starting general QA with {len(rows)} rows")
        print(f"[QA] User query: {user_query}")

        try:
            # Step 1: Infer field types from schema
           
            
            available_columns = list(rows[0].keys()) if rows else []
            
            # Build field type mapping from schema
            field_types = {}
            field_descriptions = {}
            
            for table_name, table_schema in TABLE_SCHEMAS.items():
                for col_name, col_type in table_schema["columns"].items():
                    if col_name in available_columns:
                        # Map to simple types
                        if "DATE" in col_type.upper():
                            field_types[col_name] = "date"
                        elif "NUMBER" in col_type.upper() or "NUMERIC" in col_type.upper():
                            field_types[col_name] = "number"
                        elif "VARCHAR" in col_type.upper() or "CHAR" in col_type.upper():
                            field_types[col_name] = "string"
                        else:
                            field_types[col_name] = "string"
                        
                        # Store column description
                        field_descriptions[col_name] = col_type
            
            # Fallback for columns not in schema
            for col in available_columns:
                if col not in field_types:
                    sample_val = rows[0].get(col) if rows else None
                    if isinstance(sample_val, (int, float)):
                        field_types[col] = "number"
                    elif isinstance(sample_val, str):
                        if re.match(r'\d{4}-\d{2}-\d{2}', str(sample_val)):
                            field_types[col] = "date"
                        else:
                            field_types[col] = "string"
                    else:
                        field_types[col] = "string"
            
            print(f"[QA] Inferred field types: {field_types}")
            
            # Step 2: Format data
            print("[QA] Step 2: Formatting data...")
            format_type, formatted_data = choose_optimal_format(rows, "general_qa")
            data_block = (
                f"DATA ({'CSV' if format_type == 'csv' else 'JSON'}):\n"
                f"```{format_type}\n{formatted_data}\n```"
            )

            print(f"[QA] ✅ Data formatted as {format_type.upper()} ({len(formatted_data)} chars)")

            # Step 3: Build field context
            field_context = "\n".join([
                f"- {col}: {field_types.get(col, 'unknown')} ({field_descriptions.get(col, 'no description')})"
                for col in available_columns
            ])

            # Step 4: Build QA prompt
            prompt = f"""You are a data analysis assistant helping a user understand their purchasing data.

    AVAILABLE FIELDS:
    {field_context}

    DATA CONTEXT:
    - This is purchasing/procurement data from an ERP system
    - Common fields: OrderNumber, Supplier, DatePlaced, TotalOpenAmount, Item, etc.
    - Always be precise, factual, and concise
    - Follow the user's intent exactly
    - Base all answers strictly on the provided data

    {data_block}

    USER QUESTION: "{user_query}"

    STRICT INSTRUCTIONS:

    1. **Exact String Matching:**
    - Treat each distinct value as a completely different category
    - Never group similar values (e.g., "System Accepted" ≠ "Accepted")
    - Do NOT use substring matching for categorical fields
    - All status fields, supplier names, and text values must match exactly

    2. **Numeric Analysis:**
    - "highest" / "most expensive" / "largest" → record(s) with MAX value
    - "lowest" / "cheapest" / "smallest" → record(s) with MIN value
    - "top N" → exactly N records; if ties at Nth position, include all tied
    - "bottom N" / "least N" → exactly N records; if ties at Nth position, include all tied
    - For amounts/totals, always format with commas (e.g., 1,234.56)

    3. **Date Handling:**
    - Parse dates correctly (DatePlaced, ClosedDate, RequiredDate, etc.)
    - For date ranges, include boundary dates
    - Format dates clearly (YYYY-MM-DD or MMM DD, YYYY)

    4. **Aggregations:**
    - COUNT: Number of distinct records
    - SUM: Total of numeric fields
    - AVG: Average value
    - Always show units (currency, quantities, etc.)

    5. **Output Format:**
    - Present results in a clean, readable table format
    - Use pipe separators: Column1 | Column2 | Column3
    - Include relevant columns only (don't show TenantId or internal IDs)
    - For purchase orders: OrderNumber | Supplier | DatePlaced | Amount
    - For deliveries: OrderNumber | Item | Quantity | RequiredDate
    - Adapt columns based on the question

    6. **Response Rules:**
    - NEVER infer missing data or assume values
    - If data is insufficient, state clearly what's missing
    - NO explanations unless asked
    - NO markdown headers or formatting
    - NO extra commentary
    - Return ONLY the answer

    7. **Special Cases:**
    - If question asks for count/sum/avg, provide the numeric answer
    - If question asks "how many", return a number
    - If question asks "which", return the list
    - If question is yes/no, answer directly then provide supporting data

    Your answer:"""

            # Step 5: Call model
            print("[QA] Step 3: Generating answer...")

            def generate():
                text = self.model_service.generate(self.PURPOSE, prompt).strip()
                if not text or not text.strip():
                    raise RuntimeError("Empty response from model")
                return text.strip()

            result_text = retry_with_backoff(generate, max_retries=3, initial_delay=2)

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
            # Step 1: Infer field types from schema
            
            
            available_columns = list(forecast_rows[0].keys()) if forecast_rows else []
            
            # Build field type mapping from schema
            field_types = {}
            field_descriptions = {}
            
            for table_name, table_schema in TABLE_SCHEMAS.items():
                for col_name, col_type in table_schema["columns"].items():
                    if col_name in available_columns:
                        # Map to simple types
                        if "DATE" in col_type.upper():
                            field_types[col_name] = "date"
                        elif "NUMBER" in col_type.upper() or "NUMERIC" in col_type.upper():
                            field_types[col_name] = "number"
                        elif "VARCHAR" in col_type.upper() or "CHAR" in col_type.upper():
                            field_types[col_name] = "string"
                        else:
                            field_types[col_name] = "string"
                        
                        field_descriptions[col_name] = col_type
            
            # Fallback for columns not in schema (e.g., forecast-specific columns)
            for col in available_columns:
                if col not in field_types:
                    sample_val = forecast_rows[0].get(col) if forecast_rows else None
                    if isinstance(sample_val, (int, float)):
                        field_types[col] = "number"
                    elif isinstance(sample_val, str):
                        if re.match(r'\d{4}-\d{2}-\d{2}', str(sample_val)):
                            field_types[col] = "date"
                        else:
                            field_types[col] = "string"
                    else:
                        field_types[col] = "string"
            
            print(f"[FORECAST_CHART] Inferred field types: {field_types}")
            
            # Step 2: Format data
            format_type, formatted_data = choose_optimal_format(forecast_rows, "chart")
            data_block = f"DATA ({'CSV' if format_type=='csv' else 'JSON'}):\n```{format_type}\n{formatted_data}\n```"
            
            print(f"[FORECAST_CHART] Data formatted as {format_type.upper()} ({len(formatted_data)} chars)")
            print(f"[FORECAST_CHART] CSV converted data: {data_block}")
            
            # Step 3: Identify date and metric columns
            date_cols = [col for col, typ in field_types.items() if typ == "date"]
            numeric_cols = [col for col, typ in field_types.items() if typ == "number"]
            
            date_col_hint = date_cols[0] if date_cols else "date"
            metric_col_hint = [col for col in numeric_cols if "total" in col.lower() or "amount" in col.lower()]
            metric_col_hint = metric_col_hint[0] if metric_col_hint else (numeric_cols[0] if numeric_cols else "value")
            
            print(f"[FORECAST_CHART] Detected date column: {date_col_hint}, metric column: {metric_col_hint}")
            
            # Step 4: Build field context
            field_context = "\n".join([
                f"- {col}: {field_types.get(col, 'unknown')} ({field_descriptions.get(col, 'forecast-derived')})"
                for col in available_columns
            ])
            
            # Step 5: Simplified, cleaner prompt focusing on straight lines and clarity
            prompt = f"""You are an expert data visualization specialist. Create a CLEAN, PROFESSIONAL forecast line chart.

    # DATA SCHEMA:
    {field_context}

    # ABSOLUTE REQUIREMENTS:

    ## Chart Structure
    - Type: Multi-line chart (one line per group/category)
    - X-axis: Time dimension - use column: {date_col_hint} (chronologically sorted)
    - Y-axis: Metric dimension - use column: {metric_col_hint}
    - Historical data: SOLID lines
    - Forecast data: DASHED lines (stroke-dasharray="6,4")
    - Vertical separator: Dashed line at forecast start point (identify forecast rows by 'is_forecast' flag or date boundary)

    ## Visual Design - FOLLOW EXACTLY
    - Canvas: {size}
    - Background: #0f172a (dark blue-gray)
    - Line width: 2.5px
    - **Line style: STRAIGHT segments between points** (use simple line-to commands, NOT bezier curves)
    - Data point markers: Circles, radius 5px, filled with line color
    - Grid lines: Horizontal only, color #1e293b, subtle

    ## Colors (Use these EXACT colors)
    - Line 1: #3b82f6 (blue)
    - Line 2: #10b981 (green)
    - Line 3: #f59e0b (orange)
    - Additional lines: #ef4444 (red), #8b5cf6 (purple), #06b6d4 (cyan), #ec4899 (pink)

    ## Axes Formatting
    **X-axis:**
    - Labels: Format dates as "Jan 2025", "Feb 2025", etc. (month name + year)
    - Show every month or every 2 months depending on data density
    - Font: 12px, color #94a3b8
    - Position: Bottom of chart

    **Y-axis:**
    - Format: Numbers with commas (e.g., "10,000" not "10000")
    - Use K/M suffixes for large numbers (e.g., "10K", "1.5M")
    - Font: 12px, color #94a3b8
    - Position: Left side
    - Title: "Amount ($)" or appropriate metric name - rotated -90°, position on left

    ## Legend - TOP LEFT CORNER (CRITICAL)
    - Position: Top-left corner (x: 80, y: 40)
    - Each series: Colored circle + label text
    - Layout: Vertical stack OR horizontal row (choose based on number of series)
    - Font: 13px, color #e2e8f0
    - Include: All unique group/category names from the data
    - Add separator labels: "Historical" (solid line icon) and "Forecast" (dashed line icon)

    ## Data Point Labels
    - Show values at LAST historical point and FIRST 2-3 forecast points
    - Position: Above the marker
    - Font: 11px, color matching line
    - Format: With commas and K/M suffixes

    ## Forecast Demarcation
    - Vertical dashed line at the boundary between historical and forecast
    - Color: #475569
    - Style: stroke-dasharray="4,4"
    - Optional label: "Forecast →" at top

    ## CRITICAL RULES
    1. **NO smooth curves** - use straight line segments (SVG `L` command, not `C` or `Q`)
    2. **NO overlapping lines** - each group must be clearly distinguishable
    3. **Consistent data points** - every point gets a marker circle
    4. **Clean spacing** - adequate padding (left: 80px, right: 50px, top: 70px, bottom: 60px)
    5. **Sorted data** - ensure dates are chronological before plotting
    6. **Legend in TOP LEFT** - not top right, not bottom
    7. **Identify forecast data** - look for 'is_forecast' boolean column or date threshold

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
    1. Parse data and identify unique groups/categories
    2. Sort all data points by {date_col_hint} chronologically
    3. Identify forecast boundary (where 'is_forecast' = true or based on date pattern)
    4. Calculate axis scales (min/max {metric_col_hint} with 10% padding)
    5. Draw grid lines (horizontal, subtle)
    6. Draw Y-axis with formatted labels (commas, K/M suffixes)
    7. Draw X-axis with month/year labels
    8. For each group:
    - Plot line using straight segments (L commands)
    - Add circle markers at each point
    - Use solid stroke for historical, dashed for forecast
    9. Add vertical separator at forecast boundary
    10. Draw legend in TOP LEFT corner with all groups
    11. Add selected data point labels

    Generate the clean forecast chart now (SVG only, no other text):"""

            def generate_svg():
                text = self.model_service.generate(self.PURPOSE, prompt).strip()
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
            print(f"[FORECAST_CHART] ✅ Chart generated ({len(svg)} bytes, {len(forecast_rows)} data points)")
            
            return {
                "image_b64": b64,
                "image_mime": "image/svg+xml",
                "field_types": field_types,  # Include for debugging
                "detected_date_col": date_col_hint,
                "detected_metric_col": metric_col_hint
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
    # def generate_summary(self, user_query: str, rows: List[Dict]) -> str:
    #     """Generate executive summary report using Gemini with intelligent aggregation."""
    #     if not self.enabled:
    #         print("[SUMMARY] Gemini disabled")
    #         return "⚠️ AI summary generation is currently disabled."

    #     if not rows:
    #         print("[SUMMARY] No rows")
    #         return "⚠️ No data available for summary."

    #     print(f"[SUMMARY] 📊 Starting summary generation with {len(rows)} rows")
    #     print(f"[SUMMARY] User query: {user_query}")

    #     try:
    #         # Step 1: Infer field types from schema
    #         from ivp_be.src.config.field_constant_1 import TABLE_SCHEMAS
            
    #         available_columns = list(rows[0].keys())
    #         sample_rows = rows[:3]
            
    #         # Build field type mapping from schema
    #         field_types = {}
    #         field_descriptions = {}
            
    #         for table_name, table_schema in TABLE_SCHEMAS.items():
    #             for col_name, col_type in table_schema["columns"].items():
    #                 if col_name in available_columns:
    #                     # Map to simple types
    #                     if "DATE" in col_type.upper():
    #                         field_types[col_name] = "date"
    #                     elif "NUMBER" in col_type.upper() or "NUMERIC" in col_type.upper():
    #                         field_types[col_name] = "number"
    #                     elif "VARCHAR" in col_type.upper() or "CHAR" in col_type.upper():
    #                         field_types[col_name] = "string"
    #                     else:
    #                         field_types[col_name] = "string"
                        
    #                     field_descriptions[col_name] = col_type
            
    #         # Fallback for columns not in schema
    #         for col in available_columns:
    #             if col not in field_types:
    #                 sample_val = rows[0].get(col) if rows else None
    #                 if isinstance(sample_val, (int, float)):
    #                     field_types[col] = "number"
    #                 elif isinstance(sample_val, str):
    #                     if re.match(r'\d{4}-\d{2}-\d{2}', str(sample_val)):
    #                         field_types[col] = "date"
    #                     else:
    #                         field_types[col] = "string"
    #                 else:
    #                     field_types[col] = "string"
            
    #         print(f"[SUMMARY] Inferred field types: {field_types}")
            
    #         # Step 2: Identify domain and data type
    #         domain_type = "Purchasing/Procurement"
    #         data_description = "Purchase Orders and Procurement"
            
    #         # Detect domain based on available columns
    #         if any("order" in col.lower() for col in available_columns):
    #             if any("blanket" in col.lower() for col in available_columns):
    #                 domain_type = "Blanket Order Management"
    #                 data_description = "Blanket Purchase Orders"
    #             else:
    #                 domain_type = "Purchase Order Management"
    #                 data_description = "Purchase Orders"
            
    #         if any("delivery" in col.lower() for col in available_columns):
    #             domain_type = "Purchase Order Delivery Tracking"
    #             data_description = "Purchase Order Deliveries"
            
    #         if any("supplier" in col.lower() for col in available_columns):
    #             data_description += " and Supplier Performance"
            
    #         print(f"[SUMMARY] Detected domain: {domain_type}")
            
    #         # Step 3: Generate aggregation spec
    #         print("[SUMMARY] Step 3: Generating aggregation spec...")
    #         spec_gen = SummarySpecGenerator(self.model_service)
            
    #         spec = spec_gen.get_default_spec(
    #             user_query=user_query,
    #             available_columns=available_columns,
    #             field_types=field_types,
    #             sample_rows=sample_rows
    #         )
            
    #         print(f"[SUMMARY] ✅ Spec generated with {len(spec.get('aggregations', []))} categories")
    #         print(f"[SUMMARY] Categories: {spec.get('include_categories', [])}")
    #         print(f"[SUMMARY] Summary Data Spec: {spec}")

    #         # Step 4: Aggregate the data
    #         print(f"[SUMMARY] Step 4: Aggregating {len(rows)} rows...")
    #         aggregated_summary = aggregate_for_summary(
    #             rows=rows,
    #             spec=spec,
    #             field_types=field_types
    #         )
            
    #         if "error" in aggregated_summary and aggregated_summary.get("total_records", 0) == 0:
    #             return f"⚠️ Aggregation error: {aggregated_summary['error']}"
        
    #         total_records = aggregated_summary.get("total_records", 0)
    #         print(f"[SUMMARY] ✅ Aggregated {total_records} records")
    #         print(f"[SUMMARY] Aggregation categories: {list(aggregated_summary.get('aggregations', {}).keys())}")

    #         # Step 5: Format aggregated summary for model
    #         print("[SUMMARY] Step 5: Formatting aggregated data...")
    #         summary_json = json.dumps(aggregated_summary, indent=2)
    #         data_block = f"AGGREGATED SUMMARY DATA (JSON):\n```json\n{summary_json}\n```"
    #         print(f"[SUMMARY] Aggregated summary data preview: {summary_json[:500]}...")
            
    #         print(f"[SUMMARY] ✅ Data formatted ({len(summary_json)} chars vs {len(str(rows))} original)")

    #         # Step 6: Extract metadata
    #         num_records = aggregated_summary.get("total_records", len(rows))
            
    #         # Get date range from aggregations
    #         date_info = ""
    #         time_agg = aggregated_summary.get("aggregations", {}).get("time", {})
    #         if isinstance(time_agg, dict) and "date_range" in time_agg:
    #             date_range = time_agg["date_range"]
    #             if date_range.get("min_date") and date_range.get("max_date"):
    #                 date_info = f"\n- Date Range: {date_range['min_date']} to {date_range['max_date']}"

    #         print(f"[SUMMARY] Records: {num_records}")
    #         if date_info:
    #             print(f"[SUMMARY] {date_info.strip()}")

    #         # Step 7: Build field context
    #         field_context = "\n".join([
    #             f"- {col}: {field_types.get(col, 'unknown')} ({field_descriptions.get(col, 'derived')})"
    #             for col in available_columns[:15]  # Limit to first 15 for brevity
    #         ])

    #         # Step 8: Build summary prompt with aggregated data
    #         prompt = f"""You are a senior business intelligence analyst preparing an executive summary report for {domain_type} management.

    # DATA SCHEMA:
    # {field_context}

    # CONTEXT:
    # - Domain: {domain_type}
    # - Dataset: {data_description} Records
    # - Total Records: {total_records:,} records{date_info}
    # - Data Type: Pre-aggregated summary with detailed breakdowns
    # - Analysis Focus: {domain_type.lower()} performance, spending patterns, and operational efficiency

    # USER REQUEST: "{user_query or f'Provide a comprehensive business summary and insights from this {data_description.lower()} dataset.'}"

    # {data_block}

    # ANALYSIS REQUIREMENTS:

    # ABSOLUTE OUTPUT RULES:
    # - DO NOT include any title, introduction, greeting, explanation, preface, or metadata
    # (e.g., no "Of course", no To/From/Date/Subject, no *** separators)
    # - Start directly with "## 1. Executive Summary"

    # Generate a professional business report aimed at procurement, finance, and operations leaders with the following sections:

    # ## 1. Executive Summary (2–4 sentences)
    # - Briefly describe the dataset scope using aggregations.time.date_range
    # - Highlight the single most important business finding from the aggregated data
    # - Reference key metrics from aggregations.financial or aggregations.spending (total spend, total orders, etc.)
    # - Frame findings in the context of {domain_type.lower()}

    # ## 2. Spending & Volume Overview
    # **Financial Metrics** (use aggregations.financial or aggregations.spending):
    # - **Total Spend**: Total amount across all records
    # - **Total Orders**: Number of purchase orders/deliveries
    # - **Average Order Value**: Mean transaction size
    # - **Spend Range**: Min to max values
    # - **Median Order**: Middle-point order value
    # - Interpret spending patterns and volume trends

    # **Volume Distribution**:
    # - Order count by time period (if aggregations.time exists)
    # - Spending concentration (percentage from top suppliers/locations)

    # ## 3. Supplier Performance Analysis
    # **Top Suppliers** (use aggregations.supplier or aggregations.provider):
    # - List top 5-10 suppliers by total spend
    # - Calculate supplier concentration (top 3 share of total spend)
    # - Identify supplier dependency risks
    # - Note any concerning patterns (single supplier dominance, etc.)

    # **Supplier Diversity**:
    # - Total unique suppliers
    # - Spending distribution across supplier base
    # - Opportunities for diversification

    # ## 4. Order Status & Delivery Performance
    # **Order Status Breakdown** (use aggregations.status or aggregations.order_status):
    # - Open vs Closed orders (count and value)
    # - On-time delivery rate (if delivery date data available)
    # - Backlog analysis (open orders aging)

    # **Delivery Metrics** (if aggregations.delivery exists):
    # - Total quantity ordered vs received
    # - Open quantity pending delivery
    # - Average fulfillment time
    # - Delivery reliability by supplier

    # ## 5. Location & Cost Center Analysis
    # **Spending by Location** (use aggregations.location or aggregations.cost_center):
    # - Top locations by spend
    # - Geographic concentration analysis
    # - Per-location average order value
    # - Identify high-spend vs low-spend locations

    # **Cost Allocation**:
    # - Spending patterns by business unit
    # - Budget utilization insights
    # - Cost center efficiency comparison

    # ## 6. Operational Insights & Efficiency
    # **Process Performance**:
    # - Order processing cycle time (if date fields available)
    # - Average time from order to delivery
    # - Order approval efficiency
    # - Buying patterns (frequency, timing)

    # **Procurement Efficiency**:
    # - Purchase order consolidation opportunities
    # - Maverick spend indicators
    # - Compliance with procurement policies
    # - Potential for blanket order usage

    # ## 7. Risk Factors & Issues
    # **Identified Risks**:
    # - Supplier concentration risk (dependency on few suppliers)
    # - Single-source items or services
    # - Order backlogs and delays
    # - Budget overruns or unusual spending spikes
    # - Data quality issues (missing fields, outliers)

    # **Compliance & Control**:
    # - Unauthorized spending patterns
    # - Orders outside normal ranges
    # - Missing required approvals (if approval data available)

    # ## 8. Key Recommendations (4–6 bullet points)
    # Provide concrete, action-oriented recommendations prioritized by impact:
    # - **Supplier Management**: Recommendations for supplier diversification or consolidation
    # - **Cost Optimization**: Opportunities to reduce spending or improve pricing
    # - **Process Improvement**: Streamline procurement workflows
    # - **Risk Mitigation**: Address concentration risks and backlogs
    # - **Data Quality**: Fixes for missing or inconsistent data
    # - **Strategic Actions**: Long-term initiatives for procurement excellence

    # FORMATTING RULES:
    # - Use markdown headers (##) for sections exactly as above
    # - Use **bold** for key metrics and section titles
    # - Always include specific numbers and percentages
    # - Format large numbers with commas (e.g., "1,234,567")
    # - Use currency symbols where appropriate ($, €, etc.) or "units" for quantities
    # - Keep each section concise (3–6 sentences or bullet points)
    # - Total length: 600–800 words
    # - Maintain formal, professional business tone

    # CRITICAL ANALYSIS RULES:
    # - Base ALL statements strictly on the aggregated data provided
    # - Calculate percentages and ratios to provide context
    # - Compare and contrast patterns (e.g., "Supplier A: 35% vs Supplier B: 25%")
    # - Identify trends over time if aggregations.time exists
    # - Flag outliers and anomalies
    # - NEVER make assumptions or add information not in the aggregations
    # - If data is missing for a section, state: "Insufficient data for [section name] analysis."
    # - Always relate findings back to {domain_type.lower()} best practices

    # DOMAIN-SPECIFIC FOCUS ({domain_type}):
    # - Emphasize procurement KPIs (spend under management, supplier performance, order fulfillment)
    # - Consider purchasing best practices (consolidation, competitive bidding, contract compliance)
    # - Highlight opportunities for cost savings and process optimization
    # - Address supplier relationship management
    # - Evaluate procurement policy compliance

    # Generate the comprehensive {domain_type.lower()} report now:"""

    #         # Step 9: Call model and return result
    #         print("[SUMMARY] Step 9: Generating summary report from aggregated data...")
            
    #         def generate():
    #             text = self.model_service.generate(self.PURPOSE, prompt).strip()
    #             if not text or not text.strip():
    #                 raise RuntimeError("Empty response from model")
    #             return text.strip()

    #         result_text = retry_with_backoff(generate, max_retries=3, initial_delay=2)
            
    #         if not result_text:
    #             return "⚠️ No summary could be generated from the data."

    #         print(f"[SUMMARY] ✅ Summary generated successfully ({len(result_text)} chars)")
    #         print(f"[SUMMARY] 📊 Data reduction: {len(str(rows))} chars → {len(summary_json)} chars ({round(len(summary_json)/len(str(rows))*100, 1)}%)")
            
    #         return result_text

    #     except Exception as e:
    #         print(f"[SUMMARY] ❌ Error: {e}")
    #         import traceback
    #         traceback.print_exc()
    #         return f"⚠️ Error generating summary: {str(e)}"
    
#     def generate_comparison(self, latest_invoice: Dict, previous_month_invoice: Dict, last_6_months: List[Dict]) -> str:
#         """Generate invoice comparison report."""
#         if not self.enabled:
#             print("[COMPARISON] Gemini disabled")
#             return "⚠️ AI comparison is currently disabled."
        
        
#         try:

#             print("[COMPARISON] 📊 Starting comparison generation...")
            
#             avg_grand_total = sum(inv.get("GrandTotal", 0) for inv in last_6_months) / len(last_6_months) if last_6_months else 0
#             avg_net_total = sum(inv.get("NetTotal", 0) for inv in last_6_months) / len(last_6_months) if last_6_months else 0
#             avg_tax = sum(inv.get("TotalTax", 0) for inv in last_6_months) / len(last_6_months) if last_6_months else 0
#             avg_rental = sum(inv.get("RentalCharge", 0) or 0 for inv in last_6_months) / len(last_6_months) if last_6_months else 0

        
#             prompt = f"""You are a senior telecom billing analyst preparing a formal invoice comparison report for internal finance and operations leadership.

# CONTEXT:
# - This is an internal business report, not a conversational response
# - The report will be stored, exported to PDF, and reviewed by management

# LATEST INVOICE (Current):
# {json.dumps(latest_invoice, indent=2, default=str)}

# PREVIOUS MONTH INVOICE:
# {json.dumps(previous_month_invoice, indent=2, default=str)}

# 6-MONTH HISTORICAL AVERAGES:
# - Average Grand Total: {avg_grand_total:,.2f}
# - Average Net Total: {avg_net_total:,.2f}
# - Average Tax: {avg_tax:,.2f}
# - Average Rental: {avg_rental:,.2f}

# ABSOLUTE OUTPUT RULES (MANDATORY):
# - DO NOT include any introduction, greeting, preface, or closing remarks
# - DO NOT include phrases like "Of course", "Here is", "This report shows"
# - DO NOT include titles, dates, account headers, or decorative separators
# - DO NOT use markdown emphasis such as **bold**, *, _, or ***
# - DO NOT use horizontal rules (---)
# - DO NOT invent explanations beyond the data provided
# - DO NOT use currency symbols; use numeric values only
# - DO NOT repeat raw JSON or restate the input

# FORMATTING RULES:
# - Use markdown headers (##) ONLY for section headings
# - Tables must be plain markdown tables without bold formatting
# - Percentages must be numeric only (e.g., -32.20%)
# - Use clear, professional, neutral business language
# - Base ALL calculations strictly on provided values

# REPORT STRUCTURE (FOLLOW EXACTLY):

# ## 1. Executive Summary
# - 3–4 sentences summarizing the key financial movement
# - Focus on the largest variance in Grand Total
# - Clearly state whether the change is increase or decrease and why

# ## 2. Financial Comparison

# ### Month-over-Month Comparison
# Present a table with the following columns:
# Metric | Current Month | Previous Month | Change | Change %

# Include:
# - Grand Total
# - Net Total
# - Total Tax
# - Rental Charge

# Calculations:
# - Change = Current - Previous
# - Change % = (Change / Previous) × 100

# ### Six-Month Benchmark Comparison
# Metric | Current Month | Six-Month Average | Variance | Variance %

# Include:
# - Grand Total

# ## 3. Service and Configuration Review
# Compare the following fields and state clearly if they changed or not:
# - Service Name
# - Bandwidth
# - Provider
# - Cost Center
# - Charge Per Minute

# ## 4. Root Cause Analysis
# Explain the primary drivers of the variance:
# - Service configuration changes
# - Usage changes
# - Rate or rental changes
# - Tax changes
# - Credits or adjustments

# Only list causes supported by the data.

# ## 5. Recommendations
# Provide 3–5 clear, actionable recommendations based on the findings.

# ## 6. Risk Assessment
# Classify the invoice as:
# - Low Risk: variance < 5%
# - Medium Risk: variance 5–15%
# - High Risk: variance > 15%

# Explain the classification in 2–3 sentences.

# LENGTH REQUIREMENTS:
# - 500–700 words
# - Concise, structured, factual

# Generate the report now."""

                
#                 # Generate
#             def generate():
#                 text = self.model_service.generate(self.PURPOSE, prompt).strip()
#                 if not text or not text.strip():
#                     raise RuntimeError("Empty response from model")
#                 return text.strip()

#             result_text = retry_with_backoff(generate)
                
#             if not result_text:
#                 return "⚠️ No comparison could be generated."

#             print(f"[COMPARISON] ✅ Comparison generated ({len(result_text)} chars)")
                
#             return result_text    

#         except Exception as e:
#             print(f"[COMPARISON] ❌ Error: {e}")
#             import traceback
#             traceback.print_exc()
#             return f"⚠️ Error generating comparison: {str(e)}"


