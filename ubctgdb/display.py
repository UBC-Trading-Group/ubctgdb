"""Notebook presentation, independent of pandas' text representation."""
from datetime import datetime, timezone
from html import escape
import pandas as pd


def render_tables(records, sort_by, ascending=None):
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
    if ascending is None:
        ascending = sort_by != 'updated_at'
    for rows in groups.values():
        if sort_by == 'table':
            rows.sort(key=lambda row: alphabetical(row['table']), reverse=not ascending)
        if sort_by == 'updated_at':
            rows.sort(key=updated, reverse=not ascending)
    if sort_by == 'updated_at':
        select = min if ascending else max
        folders.sort(key=lambda folder: select(map(updated, groups[folder])), reverse=not ascending)

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
                      row['size'] if 'size' in row else _file_size(row['size_bytes']), row['updated_by'])
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


class TableListing(pd.DataFrame):
    """Keep pandas data access and text output; customize only notebook HTML."""
    _metadata = ['_listing_sort', '_listing_ascending', '_listing_times']

    @property
    def _constructor(self):
        return TableListing

    def _repr_html_(self):
        required = {'table', 'rows', 'updated_at', 'size', 'updated_by'}
        if not self.columns.is_unique or not required.issubset(self.columns):
            return super()._repr_html_()
        records = self.to_dict('records')
        times = getattr(self, '_listing_times', {})
        for row in records:
            original = times.get(row['table'])
            if original and datetime.fromisoformat(original).strftime('%Y-%m-%d %H:%M') == row['updated_at']:
                row['updated_at'] = original
        return render_tables(records, getattr(self, '_listing_sort', 'table'),
                             getattr(self, '_listing_ascending', True))


class TableDescription(dict):
    """Dictionary access is unchanged; notebooks show metadata and schema tables."""
    def _repr_html_(self):
        from .core import _file_size
        html = ['<div style="text-align:left;overflow-wrap:anywhere">',
                '<h3>' + escape(str(self.get('table', ''))) + '</h3>',
                '<p>' + escape(str(self.get('description', ''))) + '</p>',
                '<table><tbody>']
        for key, label in (('rows', 'Rows'), ('columns', 'Columns'), ('size_bytes', 'Size'),
                           ('created_at', 'Created (UTC)'), ('updated_at', 'Last updated (UTC)'),
                           ('updated_by', 'Updated by')):
            value = self.get(key, '')
            if key == 'size_bytes' and isinstance(value, (int, float)):
                value = _file_size(value)
            elif key in ('created_at', 'updated_at') and value:
                value = datetime.fromisoformat(value).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M')
            html.append('<tr><th style="text-align:left;padding:6px 12px">' + label
                        + '</th><td style="text-align:left;padding:6px 12px">' + escape(str(value)) + '</td></tr>')
        html.append('</tbody></table><h4>Columns</h4><table><thead><tr><th>Column</th><th>Type</th></tr></thead><tbody>')
        for name, dtype in self.get('schema', {}).items():
            html.append('<tr><td style="text-align:left;padding:6px 12px">' + escape(str(name))
                        + '</td><td style="text-align:left;padding:6px 12px">' + escape(str(dtype)) + '</td></tr>')
        html.append('</tbody></table></div>')
        return ''.join(html)
