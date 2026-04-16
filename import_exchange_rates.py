#!/usr/bin/env python3
"""
=============================================================================
ZINIER: Import Historical Exchange Rates into Production
=============================================================================
Purpose: Fix consolidated reports (P&L, Balance Sheet, Trial Balance) by
         importing missing historical exchange rates (Jan 2015 - Oct 2025)

What this does:
  - Inserts ~10,400 records into res_currency_rate table
  - Covers 9 currencies × 10 companies × 130 months
  - Does NOT modify any journal entries, payments, or move lines

Usage:
  python3 import_exchange_rates.py \
    --db-name <database> \
    --db-user <user> \
    --db-password <password> \
    --db-host <host> \
    --db-port <port>

  Add --dry-run to preview without inserting.

Author: CloudScience Labs
Date: 2026-04-16
=============================================================================
"""

import csv
import os
import sys
import argparse
import psycopg2
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(description='Import historical exchange rates for Zinier')
    parser.add_argument('--db-name', required=True, help='Database name')
    parser.add_argument('--db-user', required=True, help='Database user')
    parser.add_argument('--db-password', default='', help='Database password')
    parser.add_argument('--db-host', default='localhost', help='Database host')
    parser.add_argument('--db-port', type=int, default=5432, help='Database port')
    parser.add_argument('--dry-run', action='store_true', help='Preview without inserting')
    parser.add_argument('--csv-file', default=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical_exchange_rates.csv'))
    args = parser.parse_args()

    print("=" * 70)
    print("ZINIER: Historical Exchange Rate Import")
    print(f"Database: {args.db_name}@{args.db_host}:{args.db_port}")
    print(f"CSV: {args.csv_file}")
    print(f"Dry Run: {args.dry_run}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 70)

    # ---- Connect ----
    conn = psycopg2.connect(
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
        host=args.db_host,
        port=args.db_port
    )
    conn.autocommit = False
    cur = conn.cursor()

    # ---- Step 1: Get companies ----
    cur.execute("""
        SELECT c.id, c.name, rc.id, rc.name
        FROM res_company c
        JOIN res_currency rc ON c.currency_id = rc.id
        ORDER BY c.id
    """)
    companies = cur.fetchall()
    print(f"\nCompanies found: {len(companies)}")
    for comp_id, comp_name, curr_id, curr_name in companies:
        print(f"  [{comp_id}] {comp_name} (currency: {curr_name})")

    # ---- Step 2: Get currency mapping ----
    target_currencies = ['AUD', 'CAD', 'CLP', 'EUR', 'GBP', 'INR', 'MXN', 'SGD', 'USD']
    cur.execute("SELECT id, name, active FROM res_currency WHERE name IN %s", (tuple(target_currencies),))
    currency_map = {}
    for cid, name, active in cur.fetchall():
        currency_map[name] = cid
        if not active:
            print(f"  WARNING: {name} (id={cid}) is INACTIVE - activating...")
            if not args.dry_run:
                cur.execute("UPDATE res_currency SET active = true WHERE id = %s", (cid,))

    print(f"\nCurrency mapping: {currency_map}")
    missing = set(target_currencies) - set(currency_map.keys())
    if missing:
        print(f"  ERROR: Missing currencies: {missing}")
        print(f"  Please create these currencies in Odoo first.")
        conn.close()
        sys.exit(1)

    # ---- Step 3: Load CSV rates ----
    rates = []
    with open(args.csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rates.append({
                'date': row['date'],
                'currency_code': row['currency_code'],
                'rate': float(row['rate']),
            })
    print(f"CSV rates loaded: {len(rates)}")

    # ---- Step 4: Pre-import state ----
    cur.execute("SELECT COUNT(*), MIN(name), MAX(name) FROM res_currency_rate")
    existing_count, existing_min, existing_max = cur.fetchone()
    print(f"Existing rates in DB: {existing_count} ({existing_min} to {existing_max})")

    # ---- Step 5: Import rates for each company ----
    total_inserted = 0
    total_updated = 0
    total_skipped = 0

    cur.execute("SELECT COALESCE(MAX(id), 0) FROM res_currency_rate")
    next_id = cur.fetchone()[0] + 1

    for comp_id, comp_name, comp_curr_id, comp_curr_name in companies:
        print(f"\n--- Company: {comp_name} (currency: {comp_curr_name}) ---")
        comp_inserted = 0
        comp_updated = 0
        comp_skipped = 0

        for rate_data in rates:
            currency_code = rate_data['currency_code']
            rate_date = rate_data['date']
            rate_value = rate_data['rate']

            if currency_code not in currency_map:
                comp_skipped += 1
                continue

            currency_id = currency_map[currency_code]

            if currency_code == comp_curr_name:
                comp_skipped += 1
                continue

            # Calculate rate relative to company's base currency
            # CSV has: 1 USD = X foreign
            # Need: 1 company_currency = Y foreign
            company_usd_rate = None
            for r in rates:
                if r['date'] == rate_date and r['currency_code'] == comp_curr_name:
                    company_usd_rate = r['rate']
                    break

            if company_usd_rate is None or company_usd_rate == 0:
                comp_skipped += 1
                continue

            adjusted_rate = rate_value / company_usd_rate

            try:
                cur.execute("""
                    SELECT id FROM res_currency_rate
                    WHERE currency_id = %s AND name = %s AND company_id = %s
                """, (currency_id, rate_date, comp_id))
                existing = cur.fetchone()

                if existing:
                    if not args.dry_run:
                        cur.execute("""
                            UPDATE res_currency_rate
                            SET rate = %s, write_date = NOW(), write_uid = 1
                            WHERE id = %s
                        """, (adjusted_rate, existing[0]))
                    comp_updated += 1
                else:
                    if not args.dry_run:
                        cur.execute("""
                            INSERT INTO res_currency_rate
                            (id, currency_id, name, rate, company_id, create_uid, create_date, write_uid, write_date)
                            VALUES (%s, %s, %s, %s, %s, 1, NOW(), 1, NOW())
                        """, (next_id, currency_id, rate_date, adjusted_rate, comp_id))
                        next_id += 1
                    comp_inserted += 1

            except Exception as e:
                print(f"    ERROR: {currency_code} on {rate_date}: {e}")
                conn.rollback()
                conn = psycopg2.connect(
                    dbname=args.db_name, user=args.db_user,
                    password=args.db_password, host=args.db_host, port=args.db_port
                )
                conn.autocommit = False
                cur = conn.cursor()

        print(f"  Inserted: {comp_inserted} | Updated: {comp_updated} | Skipped: {comp_skipped}")
        total_inserted += comp_inserted
        total_updated += comp_updated
        total_skipped += comp_skipped

    # ---- Commit ----
    if not args.dry_run:
        conn.commit()
        print("\nCOMMITTED to database!")
    else:
        conn.rollback()
        print("\nDRY RUN - nothing committed")

    # ---- Step 6: Verify ----
    cur.execute("SELECT COUNT(*) FROM res_currency_rate")
    grand_total = cur.fetchone()[0]

    print(f"\n{'=' * 70}")
    print(f"SUMMARY")
    print(f"  New records inserted: {total_inserted}")
    print(f"  Existing updated:     {total_updated}")
    print(f"  Skipped:              {total_skipped}")
    print(f"  Grand total in DB:    {grand_total}")
    print(f"{'=' * 70}")

    # ---- Spot check ----
    print("\nSPOT CHECK (Zinier Consol rates):")
    cur.execute("""
        SELECT rcr.name, rc.name, rcr.rate
        FROM res_currency_rate rcr
        JOIN res_currency rc ON rcr.currency_id = rc.id
        JOIN res_company comp ON rcr.company_id = comp.id
        WHERE comp.name = 'Zinier Consol'
          AND rc.name IN ('INR', 'GBP', 'SGD', 'MXN')
          AND rcr.name IN ('2020-01-01', '2023-01-01', '2024-01-01')
        ORDER BY rcr.name, rc.name
    """)
    for date, curr, rate in cur.fetchall():
        print(f"  {date} | {curr} = {rate:.4f}")

    conn.close()
    print("\nDONE!")

if __name__ == '__main__':
    main()
