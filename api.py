from google.cloud import bigtable
from google.cloud.bigtable import row_filters
from google.cloud.bigtable.row import DirectRow

project_id = "concise-flame-450703-h0"
instance_id = "ev-bigtable"
table_id = "ev-population"

client = bigtable.Client(project=project_id, admin=True)
table = client.instance(instance_id).table(table_id)

from django.contrib import admin
from django.urls import path
from django.http import JsonResponse, HttpResponse

def total_entries(request):
    row_count = 0
    for row in table.read_rows():
        row_count += 1
    return HttpResponse(str(row_count))

def best_bmw(request):
    count = 0
    rows = table.read_rows()

    for row in rows:
        make = None
        electric_range = None

        for column_family_name, columns in row.cells.items():
            for column_name, cell_list in columns.items():
                if column_name == b'make':
                    make = cell_list[0].value.decode('utf-8')
                elif column_name == b'electric_range':
                    try:
                        raw_val = cell_list[0].value.decode('utf-8').strip()
                        if raw_val:
                            electric_range = int(raw_val)
                    except (ValueError, IndexError, AttributeError):
                        continue

        if make == 'BMW' and electric_range > 100:
            count += 1

    return HttpResponse(str(count))
    
def tesla_owners(request):
    family_filter = row_filters.FamilyNameRegexFilter("ev_info")

    make_filter = row_filters.RowFilterChain(filters=[
        row_filters.ColumnQualifierRegexFilter(b"make"),
        row_filters.ValueRegexFilter(b"TESLA")
    ])

    city_filter = row_filters.RowFilterChain(filters=[
        row_filters.ColumnQualifierRegexFilter(b"city"),
        row_filters.ValueRegexFilter(b"Seattle")
    ])

    combined_filter = row_filters.RowFilterChain(filters=[
        family_filter,
        row_filters.RowFilterUnion(filters=[make_filter, city_filter])
    ])

    rows = table.read_rows(filter_=combined_filter)

    count = 0
    for row in rows:
        cells = row.cells.get("ev_info", {})
        make_cell = cells.get(b"make", [])
        city_cell = cells.get(b"city", [])

        make = make_cell[0].value if make_cell else b""
        city = city_cell[0].value if city_cell else b""

        print(f"Row Key: {row.row_key}")
        print(f"  make: {make}")
        print(f"  city: {city}")

        if make == b"TESLA" and city == b"Seattle":
            count += 1

    return HttpResponse(count)

def update(request):
    row = table.direct_row('257246118')

    row.set_cell(
        column_family_id='ev_info',
        column='electric_range',
        value=str(200),
    )

    row.commit()
    return HttpResponse('Success')

def delete(request):
    filter_model_year = row_filters.ChainFilter([
        row_filters.ColumnQualifierRegexFilter('model_year'),
        row_filters.ValueRangeFilter(end_value='2013', inclusive_end=True)
    ])

    rows = table.read_rows(filter_=filter_model_year)
    rows.consume_all()

    for row_key in rows.rows.keys():
        direct_row = DirectRow(row_key=row_key, table=table)
        direct_row.delete()
        direct_row.commit()

    return total_entries(request)

urlpatterns = [
    path('admin/', admin.site.urls),
    path('delete/', delete),
    path('update/', update),
    path('testla-owners', tesla_owners),
    path('Best-BMW/', best_bmw),
    path('rows/', total_entries)
]