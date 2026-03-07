#!/usr/bin/env python3
"""
Convert markdown report to beautiful HTML with styled tables
"""
import re
import sys

def md_to_html(md_content):
    """Convert markdown to HTML with beautiful table styling"""
    
    # CSS Styling
    css = """
<style>
    body {
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
        line-height: 1.6;
        max-width: 1200px;
        margin: 0 auto;
        padding: 40px 20px;
        background: #f8f9fa;
        color: #2d3748;
    }
    h1 {
        color: #1a202c;
        border-bottom: 4px solid #667eea;
        padding-bottom: 15px;
        margin-bottom: 30px;
        font-size: 2.5em;
    }
    h2 {
        color: #2d3748;
        margin-top: 40px;
        padding: 15px 0 15px 20px;
        border-left: 5px solid #667eea;
        background: linear-gradient(90deg, #f0f4ff 0%, transparent 100%);
    }
    h3 {
        color: #4a5568;
        margin-top: 30px;
        padding-bottom: 10px;
        border-bottom: 2px solid #e2e8f0;
    }
    table {
        border-collapse: collapse;
        width: 100%;
        margin: 25px 0;
        background: white;
        box-shadow: 0 2px 15px rgba(0,0,0,0.1);
        border-radius: 8px;
        overflow: hidden;
    }
    thead {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
    }
    th {
        padding: 16px;
        text-align: left;
        font-weight: 600;
        font-size: 0.95em;
        letter-spacing: 0.5px;
    }
    td {
        padding: 14px 16px;
        border-bottom: 1px solid #e2e8f0;
    }
    tbody tr:nth-child(even) {
        background-color: #f7fafc;
    }
    tbody tr:hover {
        background-color: #edf2f7;
        transition: background-color 0.2s;
    }
    .status-excellent {
        background-color: #48bb78 !important;
        color: white !important;
        font-weight: bold;
        text-align: center;
        border-radius: 4px;
        padding: 8px 12px !important;
    }
    .status-acceptable {
        background-color: #ed8936 !important;
        color: white !important;
        font-weight: bold;
        text-align: center;
        border-radius: 4px;
        padding: 8px 12px !important;
    }
    .status-critical {
        background-color: #f56565 !important;
        color: white !important;
        font-weight: bold;
        text-align: center;
        border-radius: 4px;
        padding: 8px 12px !important;
    }
    code {
        background: #edf2f7;
        padding: 2px 6px;
        border-radius: 3px;
        font-family: 'Courier New', monospace;
        font-size: 0.9em;
    }
    a {
        color: #667eea;
        text-decoration: none;
        font-weight: 500;
    }
    a:hover {
        text-decoration: underline;
    }
    hr {
        border: none;
        border-top: 2px solid #e2e8f0;
        margin: 40px 0;
    }
    ul {
        padding-left: 25px;
    }
    li {
        margin: 8px 0;
    }
    .number-cell {
        text-align: right !important;
        font-variant-numeric: tabular-nums;
    }
    @media print {
        body { background: white; }
        table { box-shadow: none; border: 1px solid #ddd; }
    }
</style>
"""
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Migration Quality Assessment Report</title>
    {css}
</head>
<body>
"""
    
    # Convert markdown to HTML
    lines = md_content.split('\n')
    in_table = False
    table_headers = []
    
    for line in lines:
        # Headers
        if line.startswith('# '):
            html += f'<h1>{line[2:]}</h1>\n'
        elif line.startswith('## '):
            html += f'<h2>{line[3:]}</h2>\n'
        elif line.startswith('### '):
            html += f'<h3>{line[4:]}</h3>\n'
        # Horizontal rule
        elif line.strip() == '---':
            html += '<hr>\n'
        # Table detection
        elif line.startswith('|') and not line.startswith('|---'):
            if not in_table:
                # Table header
                in_table = True
                table_headers = [cell.strip() for cell in line.split('|')[1:-1]]
                html += '<table>\n<thead>\n<tr>\n'
                for header in table_headers:
                    html += f'<th>{header}</th>\n'
                html += '</tr>\n</thead>\n<tbody>\n'
            elif in_table and '|---' not in line:
                # Table row
                cells = [cell.strip() for cell in line.split('|')[1:-1]]
                html += '<tr>\n'
                for i, cell in enumerate(cells):
                    # Apply status styling - only for cells with emoji indicators
                    cell_class = ''
                    if '✅' in cell:
                        cell_class = ' class="status-excellent"'
                    elif '⚠️' in cell:
                        cell_class = ' class="status-acceptable"'
                    elif '❌' in cell:
                        cell_class = ' class="status-critical"'
                    # Number alignment - must be ONLY digits, commas, periods, or σ
                    # Excludes field names like "LINE_ICD9_DGNS_CD" that contain digits
                    elif re.match(r'^[\d,.\-]+$', cell.strip()) or (cell.strip() and re.match(r'^[\d,.\-]+σ$', cell.strip())):
                        cell_class = ' class="number-cell"'
                    
                    html += f'<td{cell_class}>{cell}</td>\n'
                html += '</tr>\n'
        elif in_table and not line.strip().startswith('|'):
            # End of table
            html += '</tbody>\n</table>\n'
            in_table = False
            table_headers = []
            # Continue processing this line
            if line.strip():
                html += format_regular_line(line)
        # Lists
        elif line.strip().startswith('- '):
            if not html.endswith('</li>\n') and not html.endswith('<ul>\n'):
                html += '<ul>\n'
            html += f'<li>{format_inline(line[2:])}</li>\n'
        elif html.endswith('</li>\n') and not line.strip().startswith('- '):
            html += '</ul>\n'
            if line.strip():
                html += format_regular_line(line)
        # Regular paragraphs
        elif line.strip():
            html += format_regular_line(line)
        else:
            html += '\n'
    
    # Close any open table
    if in_table:
        html += '</tbody>\n</table>\n'
    
    html += """
</body>
</html>
"""
    return html

def format_inline(text):
    """Format inline markdown (bold, links, code)"""
    # Bold
    text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
    # Links
    text = re.sub(r'\[(.+?)\]\((.+?)\)', r'<a href="\2">\1</a>', text)
    # Inline code
    text = re.sub(r'`(.+?)`', r'<code>\1</code>', text)
    return text

def format_regular_line(line):
    """Format a regular line of text"""
    if line.strip():
        return f'<p>{format_inline(line)}</p>\n'
    return '\n'

if __name__ == '__main__':
    # Read markdown file
    with open('report.md', 'r') as f:
        md_content = f.read()
    
    # Convert to HTML
    html_content = md_to_html(md_content)
    
    # Write HTML file
    with open('report.html', 'w') as f:
        f.write(html_content)
    
    print("✅ HTML report generated: report.html")
    print("   - Beautiful styled tables with color-coded status")
    print("   - Professional gradient headers")
    print("   - Hover effects and proper number alignment")
    print("   - Print-friendly styling")
