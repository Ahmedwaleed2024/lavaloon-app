# Copyright (c) 2024, ahmed waleed and contributors
# For license information, please see license.txt

import frappe
from frappe import _

CUSTOM_COLLECT_STATUS = 'UnCollected'
REQUIRED_FILTERS = ["salesperson", "from_date", "to_date"]

def execute(filters=None):
    if not filters:
        filters = {}

    # Validate the required filters are present
    validate_filters(filters)

    columns = get_columns(filters)
    data = get_data(filters)

    return columns, data


def validate_filters(filters):
    """Validate that all required filters are provided."""
    for f in REQUIRED_FILTERS:
        if not filters.get(f):
            frappe.throw(_("The {0} filter is mandatory").format(frappe.bold(f.title())))


def get_columns(filters):
    """Dynamically define columns based on items in sales invoices."""
    # Get distinct item codes from sales invoices based on provided filters
    item_columns = frappe.db.sql("""
        SELECT DISTINCT sii.item_code
        FROM `tabSales Invoice Item` sii
        JOIN `tabSales Invoice` si ON sii.parent = si.name
        WHERE si.docstatus = 1 AND si.outstanding_amount = 0 
        AND si.is_return = 0 AND si.custom_collect_status = %(custom_collect_status)s
    """, {"custom_collect_status": CUSTOM_COLLECT_STATUS}, as_dict=True)

    columns = [
        {"label": _("Customer"), "fieldname": "customer", "fieldtype": "Link", "options": "Customer", "width": 150}
    ]

    for item in item_columns:
        columns.append({"label": item['item_code'], "fieldname": item['item_code'], "fieldtype": "Float", "width": 150})

    return columns


def get_data(filters):
    """Fetch sales data based on the filters and transform it into a matrix format."""
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
    conditions.append("si.custom_collect_status = %(custom_collect_status)s")
    params["custom_collect_status"] = CUSTOM_COLLECT_STATUS

    condition_string = " AND ".join(conditions) if conditions else "1=1"

    query = f"""
        SELECT
            si.customer_name AS customer,
            sii.item_code AS item,
            SUM(sii.qty) AS quantity_sold
        FROM
            `tabSales Invoice Item` sii
        JOIN
            `tabSales Invoice` si ON sii.parent = si.name
        JOIN
            `tabSales Team` st ON si.name = st.parent
        WHERE
            si.docstatus = 1 AND  si.is_return = 0  AND {condition_string}
        GROUP BY
            si.customer, sii.item_code
    """

    try:
        results = frappe.db.sql(query, params, as_dict=True)
    except Exception as e:
        frappe.log_error(_("Error fetching data: {0}").format(str(e)), "Customer item matrix report")
        return []

    return transform_to_matrix(results)


def transform_to_matrix(results):
    """Transform the results into a customer-item matrix format."""
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
