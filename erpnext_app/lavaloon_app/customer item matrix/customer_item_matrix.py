# lavaloon_app/report/customer_item_matrix.py

import frappe
from frappe import _

def execute(filters=None):
    if not filters:
        filters = {}

    # Validate the required filters are present
    validate_filters(filters)

    columns = get_columns(filters)
    data = get_data(filters)

    return columns, data

def validate_filters(filters):
    required_filters = ["salesperson", "from_date", "to_date"]
    for f in required_filters:
        if not filters.get(f):
            frappe.throw(_("The {0} filter is mandatory").format(frappe.bold(f.title())))

def get_columns(filters):
    # Define columns dynamically based on items
    item_columns = frappe.db.sql("""
        SELECT DISTINCT item_code
        FROM `tabSales Invoice Item`
        WHERE docstatus = 1 AND parent IN (
            SELECT name FROM `tabSales Invoice` WHERE docstatus = 1 AND outstanding_amount = 0
        )
    """, as_dict=True)

    columns = [
        {"label": _("Customer"), "fieldname": "customer", "fieldtype": "Data", "width": 150}
    ]

    for item in item_columns:
        columns.append({"label": item['item_code'], "fieldname": item['item_code'], "fieldtype": "Float", "width": 150})

    return columns

def get_data(filters):
    conditions = []
    params = {}

    if filters.get("from_date"):
        conditions.append("si.posting_date >= %(from_date)s")
        params["from_date"] = filters["from_date"]
    if filters.get("to_date"):
        conditions.append("si.posting_date <= %(to_date)s")
        params["to_date"] = filters["to_date"]
    if filters.get("salesperson"):
        conditions.append("st.sales_person = %(salesperson)s")
        params["salesperson"] = filters["salesperson"]

    conditions.append("si.outstanding_amount = 0")

    condition_string = " AND ".join(conditions) if conditions else "1=1"

    query = f"""
        SELECT
            si.customer AS customer,
            sii.item_code AS item,
            SUM(sii.qty) AS quantity_sold
        FROM
            `tabSales Invoice Item` sii
        JOIN
            `tabSales Invoice` si ON sii.parent = si.name
        JOIN
            `tabSales Team` st ON si.name = st.parent
        WHERE
            si.custom_collect_status = 'UnCollected' 
            And si.docstatus = 1 AND {condition_string}
        GROUP BY
            si.customer, sii.item_code
    """
    
    try:
        results = frappe.db.sql(query, params, as_dict=True)
    except Exception as e:
        frappe.log_error(_("Error fetching data: {0}").format(str(e)), "Territory-wise Sales Report")
        return []

    return transform_to_matrix(results)

def transform_to_matrix(results):
    from collections import defaultdict
    matrix = defaultdict(lambda: defaultdict(float))

    # Aggregate data by customer and item
    for row in results:
        matrix[row['customer']][row['item']] += row['quantity_sold']

    data = []
    # Prepare customer-wise data
    for customer, items in matrix.items():
        row = {"customer": customer}
        for item, qty in items.items():
            row[item] = qty
        data.append(row)

    return data
