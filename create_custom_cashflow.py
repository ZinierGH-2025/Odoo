#!/usr/bin/env python3
"""
Create Custom Cash Flow Statement for Zinier Consol
Uses the same formula-based approach as Custom_Profit and Loss (id=34)
and Custom_Balance Sheet (id=35)

This fixes the issue where the standard Odoo Cash Flow engine pulls
only one side of transactions at consolidated level.
"""
import psycopg2
import sys

DB_NAME = sys.argv[1] if len(sys.argv) > 1 else 'zinier-odoo-uat-29967538'

conn = psycopg2.connect(dbname=DB_NAME)
conn.autocommit = False
cur = conn.cursor()

# Check if Custom Cash Flow already exists
cur.execute("SELECT id FROM account_report WHERE name::text ILIKE '%Custom_Cash Flow%'")
existing = cur.fetchone()
if existing:
    print(f"Custom_Cash Flow already exists with id={existing[0]}. Skipping creation.")
    conn.close()
    sys.exit(0)

# Get next IDs
cur.execute("SELECT COALESCE(MAX(id),0)+1 FROM account_report")
report_id = cur.fetchone()[0]
cur.execute("SELECT COALESCE(MAX(id),0)+1 FROM account_report_line")
next_line_id = cur.fetchone()[0]
cur.execute("SELECT COALESCE(MAX(id),0)+1 FROM account_report_expression")
next_expr_id = cur.fetchone()[0]

print(f"Creating Custom Cash Flow Statement (report_id={report_id})")
print(f"Starting line_id={next_line_id}, expr_id={next_expr_id}")

# ============================================================
# 1. Create the report
# ============================================================
cur.execute("""
INSERT INTO account_report (id, name, root_report_id, country_id,
    create_uid, create_date, write_uid, write_date,
    filter_multi_company, filter_date_range, filter_analytic,
    filter_unfold_all, filter_journals, filter_period_comparison)
VALUES (%s, '{"en_US": "Custom_Cash Flow Statement"}'::jsonb, NULL, NULL,
    1, NOW(), 1, NOW(),
    'selector', true, false, false, false, true)
""", (report_id,))
print(f"  Report created: id={report_id}")

# ============================================================
# 2. Create report lines and expressions
# ============================================================
lines = []  # (id, name, code, sequence, parent_id)
exprs = []  # (id, label, engine, formula, report_line_id)

lid = next_line_id
eid = next_expr_id

def add_line(name, code, seq, parent_id=None):
    global lid
    lines.append((lid, name, code, seq, parent_id))
    current_lid = lid
    lid += 1
    return current_lid

def add_expr(label, engine, formula, line_id):
    global eid
    exprs.append((eid, label, engine, formula, line_id))
    eid += 1

# ---- OPERATING ACTIVITIES ----
op_id = add_line('Operating Activities', 'CF_OP', 1)

# Net Income (references Custom P&L NEP code)
ni_id = add_line('Net Income', 'CF_NI', 2, op_id)
add_expr('balance', 'aggregation', 'NEP.balance', ni_id)

# Adjustments for non-cash items
adj_id = add_line('Adjustments for Non-Cash Items', 'CF_ADJ', 3, op_id)

# Depreciation & Amortization (add back - these are expenses with positive balance)
dep_id = add_line('Depreciation & Amortization', 'CF_DEP', 4, adj_id)
add_expr('balance', 'account_codes', '68500 + 68501 + 68507', dep_id)

# Unrealized FX Gains/Losses
fx_id = add_line('Unrealized FX Gains/Losses', 'CF_FX', 5, adj_id)
add_expr('balance', 'account_codes', '92010 + 92030', fx_id)

# Total Adjustments
tadj_id = add_line('Total Adjustments', 'CF_TADJ', 6, adj_id)
add_expr('balance', 'aggregation', 'CF_DEP.balance + CF_FX.balance', tadj_id)

# Changes in Working Capital
wc_id = add_line('Changes in Working Capital', 'CF_WC', 7, op_id)

# Change in Receivables (increase in receivable = cash used, so negate)
rec_id = add_line('Change in Trade Receivables', 'CF_REC', 8, wc_id)
add_expr('balance', 'domain',
    "[('account_id.code', 'in', ['12100','12199','12590','12591','12599'])]", rec_id)

# Change in Unbilled Receivables
ubr_id = add_line('Change in Unbilled Receivables', 'CF_UBR', 9, wc_id)
add_expr('balance', 'domain',
    "[('account_id.code', 'in', ['12710','12730'])]", ubr_id)

# Change in Prepaid Expenses
pre_id = add_line('Change in Prepaid Expenses', 'CF_PRE', 10, wc_id)
add_expr('balance', 'domain',
    "[('account_id.code', 'in', ['13000','13100','13199'])]", pre_id)

# Change in Other Current Assets
oca_id = add_line('Change in Other Current Assets', 'CF_OCA', 11, wc_id)
add_expr('balance', 'domain',
    "[('account_id.code', 'in', ['1010','12900','14100','14110','14200','14730','14740','14800','14810','14811','14812','14813','14816','14817','14820'])]", oca_id)

# Change in Accounts Payable
ap_id = add_line('Change in Accounts Payable', 'CF_AP', 12, wc_id)
add_expr('balance', 'domain',
    "[('account_id.account_type', '=', 'liability_payable'), ('account_id.non_trade', '=', False)]", ap_id)

# Change in Credit Card
cc_id = add_line('Change in Credit Card', 'CF_CC', 13, wc_id)
add_expr('balance', 'domain', "[('account_id.code', '=', '21120')]", cc_id)

# Change in Deferred Revenue
dr_id = add_line('Change in Deferred Revenue', 'CF_DR', 14, wc_id)
add_expr('balance', 'domain',
    "[('account_id.code', 'in', ['22100','22500','22600','22901'])]", dr_id)

# Change in Other Current Liabilities
ocl_id = add_line('Change in Other Current Liabilities', 'CF_OCL', 15, wc_id)
add_expr('balance', 'domain',
    "[('account_id.code', 'in', ['24700','24800'])]", ocl_id)

# Change in Accruals
acr_id = add_line('Change in Accruals', 'CF_ACR', 16, wc_id)
add_expr('balance', 'domain', "[('account_id.code', '=', '24110')]", acr_id)

# Total Working Capital Changes
twc_id = add_line('Total Working Capital Changes', 'CF_TWC', 17, wc_id)
add_expr('balance', 'aggregation',
    'CF_REC.balance + CF_UBR.balance + CF_PRE.balance + CF_OCA.balance + CF_AP.balance + CF_CC.balance + CF_DR.balance + CF_OCL.balance + CF_ACR.balance', twc_id)

# Total Operating Activities
top_id = add_line('Net Cash from Operating Activities', 'CF_TOP', 18, op_id)
add_expr('balance', 'aggregation', 'CF_NI.balance + CF_TADJ.balance + CF_TWC.balance', top_id)

# ---- INVESTING ACTIVITIES ----
inv_id = add_line('Investing Activities', 'CF_INV', 19)

# Change in Fixed Assets
fa_id = add_line('Change in Fixed Assets', 'CF_FA', 20, inv_id)
add_expr('balance', 'domain', "[('account_id.account_type', '=', 'asset_fixed')]", fa_id)

# Change in Non-current Assets
nca_id = add_line('Change in Non-current Assets', 'CF_NCA', 21, inv_id)
add_expr('balance', 'domain', "[('account_id.account_type', '=', 'asset_non_current')]", nca_id)

# Total Investing Activities
tinv_id = add_line('Net Cash from Investing Activities', 'CF_TINV', 22, inv_id)
add_expr('balance', 'aggregation', 'CF_FA.balance + CF_NCA.balance', tinv_id)

# ---- FINANCING ACTIVITIES ----
fin_id = add_line('Financing Activities', 'CF_FIN', 23)

# Change in Non-current Liabilities
ncl_id = add_line('Change in Non-current Liabilities', 'CF_NCL', 24, fin_id)
add_expr('balance', 'domain', "[('account_id.account_type', '=', 'liability_non_current')]", fin_id)

# Change in Equity
eq_id = add_line('Change in Equity', 'CF_EQ', 25, fin_id)
add_expr('balance', 'domain', "[('account_id.account_type', '=', 'equity')]", eq_id)

# Total Financing Activities
tfin_id = add_line('Net Cash from Financing Activities', 'CF_TFIN', 26, fin_id)
add_expr('balance', 'aggregation', 'CF_NCL.balance + CF_EQ.balance', tfin_id)

# ---- NET CHANGE IN CASH ----
net_id = add_line('Net Change in Cash', 'CF_NET', 27)
add_expr('balance', 'aggregation', 'CF_TOP.balance + CF_TINV.balance + CF_TFIN.balance', net_id)

# ---- CASH BALANCES ----
# Opening Cash (uses balance at start of period)
open_id = add_line('Opening Cash Balance', 'CF_OPEN', 28)
add_expr('balance', 'domain', "[('account_id.account_type', '=', 'asset_cash')]", open_id)

# Closing Cash Balance
close_id = add_line('Closing Cash Balance', 'CF_CLOSE', 29)
add_expr('balance', 'aggregation', 'CF_OPEN.balance + CF_NET.balance', close_id)

# ============================================================
# 3. Insert all lines
# ============================================================
print(f"\nInserting {len(lines)} report lines...")
for (line_id, name, code, seq, parent_id) in lines:
    cur.execute("""
    INSERT INTO account_report_line
        (id, report_id, name, code, sequence, parent_id,
         groupby, create_uid, create_date, write_uid, write_date)
    VALUES (%s, %s, %s, %s, %s, %s,
            NULL, 1, NOW(), 1, NOW())
    """, (line_id, report_id,
          '{"en_US": "' + name + '"}'
          if '{' not in name else name,
          code, seq, parent_id))
    print(f"  Line {line_id}: {name} (code={code}, seq={seq})")

# ============================================================
# 4. Insert all expressions
# ============================================================
print(f"\nInserting {len(exprs)} expressions...")
for (expr_id, label, engine, formula, line_id) in exprs:
    cur.execute("""
    INSERT INTO account_report_expression
        (id, report_line_id, label, engine, formula,
         create_uid, create_date, write_uid, write_date)
    VALUES (%s, %s, %s, %s, %s,
            1, NOW(), 1, NOW())
    """, (expr_id, line_id, label, engine, formula))
    print(f"  Expr {expr_id}: {label}/{engine} -> {formula[:60]}")

# ============================================================
# 5. Commit
# ============================================================
conn.commit()
print(f"\n=== COMMITTED: Custom Cash Flow Statement created ===")
print(f"Report ID: {report_id}")
print(f"Lines: {len(lines)}")
print(f"Expressions: {len(exprs)}")
print(f"\nTo view: Accounting -> Reporting -> Custom_Cash Flow Statement")

conn.close()
print("DONE!")
