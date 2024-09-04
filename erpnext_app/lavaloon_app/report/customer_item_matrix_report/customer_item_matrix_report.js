// Copyright (c) 2024, ahmed waleed and contributors
// For license information, please see license.txt

frappe.query_reports["Customer Item Matrix Report"] = {
	"filters": [
        {
            "label": __("Salesperson"),
            "fieldname": "salesperson",
            "fieldtype": "Link",
            "options": "Sales Person",
            "default": "",
            "reqd": 1
        },
        {
            "label": __("From Date"),
            "fieldname": "from_date",
            "fieldtype": "Date",
            "default": "",
            "reqd": 1
        },
        {
            "label": __("To Date"),
            "fieldname": "to_date",
            "fieldtype": "Date",
            "default": "",
            "reqd": 1
        }
    ],

    "get_data": function(filters) {
        return frappe.call({
            method: "frappe.client.get_list",
            args: {
                doctype: "Sales Invoice",
                fields: ["name"],
                filters: {
                    docstatus: 1,
                    custom_collect_status: "UnCollected", // Added filter for custom_collect_status
                    ...(filters.from_date ? { posting_date: [">=", filters.from_date] } : {}),
                    ...(filters.to_date ? { posting_date: ["<=", filters.to_date] } : {}),
                    ...(filters.salesperson ? { sales_person: filters.salesperson } : {})
                },
                limit_page_length: 1000
            }
        }).then(response => {
            let invoices = response.message || [];
            let invoice_names = invoices.map(invoice => invoice.name);

            if (invoice_names.length === 0) {
                return [];
            }

            return frappe.call({
                method: "frappe.client.get_list",
                args: {
                    doctype: "Sales Invoice Item",
                    fields: ["parent", "item_code", "qty"],
                    filters: {
                        parent: ["in", invoice_names],
                        docstatus: 1
                    },
                    limit_page_length: 1000
                }
            }).then(response => {
                let sales_data = response.message || [];
                let data = [];
                let customer_map = {};

                sales_data.forEach(item => {
                    let customer = get_customer(item.parent);
                    if (!customer_map[customer]) {
                        customer_map[customer] = {};
                    }
                    if (!customer_map[customer][item.item_code]) {
                        customer_map[customer][item.item_code] = 0;
                    }
                    customer_map[customer][item.item_code] += item.qty;
                });

                for (let [customer, items] of Object.entries(customer_map)) {
                    let row = { customer: customer };
                    for (let [item, qty] of Object.entries(items)) {
                        row[item] = qty;
                    }
                    data.push(row)
                }

                return data;
            }).catch(error => {
                console.error("Error fetching sales invoice items:", error);
                return [];
            });
        }).catch(error => {
            console.error("Error fetching sales invoices:", error);
            return [];
        });
    }
};

async function get_customer(invoice_name) {
    try {
        let response = await frappe.call({
            method: "frappe.client.get_list",
            args: {
                doctype: "Sales Invoice",
                fields: ["customer"],
                filters: {
                    name: invoice_name
                },
                limit_page_length: 1
            }
        });

        let customer_list = response.message || [];
        return customer_list.length > 0 ? customer_list[0].customer : "Unknown";
    } catch (error) {
        console.error("Error fetching customer:", error);
        return "Unknown";
    }
}
