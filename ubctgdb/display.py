"""Notebook presentation, independent of pandas' text representation."""
from datetime import datetime, timezone
from html import escape


def render_tables(records, sort_by):
    from .core import _file_size

    def alphabetical(value):
        return value.casefold(), value

    def updated(row):
        return datetime.fromisoformat(row['updated_at']).astimezone(timezone.utc)

    groups = {}
    for row in records:
        folder = row['table'].rpartition('/')[0]
        groups.setdefault(folder, []).append(row)
    folders = sorted(groups, key=alphabetical)
    for rows in groups.values():
        rows.sort(key=lambda row: alphabetical(row['table']))
        if sort_by == 'updated_at':
            rows.sort(key=updated, reverse=True)
    if sort_by == 'updated_at':
        folders.sort(key=lambda folder: max(map(updated, groups[folder])), reverse=True)

    html = ['<div style="overflow-x:auto"><table style="width:100%;table-layout:fixed;border-collapse:collapse;text-align:left">',
            '<caption style="text-align:left;padding:8px 0">Shared tables</caption>',
            '<colgroup><col style="width:36%"><col style="width:12%"><col style="width:22%"><col style="width:12%"><col style="width:18%"></colgroup>',
            '<thead><tr>']
    for label in ('Table', 'Rows', 'Last updated (UTC)', 'Size', 'Updated by'):
        html.append('<th scope="col" style="padding:8px;text-align:left">' + label + '</th>')
    html.append('</tr></thead><tbody>')
    if not records:
        html.append('<tr><td colspan="5" style="padding:12px">No tables found.</td></tr>')
    for folder in folders:
        label = folder or 'Top level'
        html.append('<tr><th scope="rowgroup" colspan="5" style="text-align:left;padding:10px 8px;background:rgba(128,128,128,.15);overflow-wrap:anywhere">'
                    + escape(label) + '</th></tr>')
        for row in groups[folder]:
            values = (row['table'].rsplit('/', 1)[-1],
                      '' if row['rows'] is None else f"{row['rows']:,}",
                      updated(row).strftime('%Y-%m-%d %H:%M'),
                      _file_size(row['size_bytes']), row['updated_by'])
            html.append('<tr>')
            for index, value in enumerate(values):
                align = 'right' if index in (1, 3) else 'left'
                padding = '8px 8px 8px 24px' if index == 0 else '8px'
                title = ' title="' + escape(row['table'], quote=True) + '"' if index == 0 else ''
                html.append(f'<td{title} style="padding:{padding};text-align:{align};overflow-wrap:anywhere">'
                            + escape(str(value)) + '</td>')
            html.append('</tr>')
    html.append('</tbody></table></div>')
    return ''.join(html)
