from __future__ import annotations


def build_chart_prompt(
    user_query: str,
    data_block: str,
    size: str = "960x560",
) -> str:

    return f"""You are an expert data visualization specialist. Your task is to analyze the user's request and data, then create the most appropriate chart type.

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
# - ex: line chart of net total and rental charge against cost name, it should have 2 lines

# **Bar/Column Charts:**
# - Adequate spacing between bars
# - Values displayed on bars
# - Bars should be proportional to values

# **Pie Charts:**
# - Show both percentage and value
# - Labels with leader lines if needed
# - Start at 12 o'clock position
# - Slice should be proper triangle
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