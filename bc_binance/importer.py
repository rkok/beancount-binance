import csv
import re
import sys
from operator import itemgetter
from os import path

import dateutil.parser

from beancount.core import amount as bc_amount, position, data
from beancount.core.number import D
from beancount.ingest import importer


class Importer(importer.ImporterProtocol):
    """An importer for Binance using a custom CSV file, generated from Binance order/trade CSVs and API data."""

    def __init__(self, asset_account, commission_account, **kwargs):
        super().__init__(**kwargs)
        self.asset_account = asset_account
        self.commission_account = commission_account
        self.lots = []

    def identify(self, file):
        return self.ident_orders_file(file) or self.ident_statement_file(file)

    def ident_orders_file(self, file):
        filename = path.basename(file.name)
        return re.match(r"^bina-.*-processed\.csv$", filename)

    def ident_statement_file(self, file):
        filename = path.basename(file.name)
        return re.match(r"^0binastmt-.*\.csv$", filename)

    def file_account(self, file):
        return ""

    def extract(self, file, existing_entries=None):
        transactions = []

        is_orders_file = self.ident_orders_file(file)
        is_stmt_file = self.ident_statement_file(file)

        with open(file.name, "r") as f:
            csv_reader = csv.reader(f, delimiter=',')
            i = 0
            for row in csv_reader:
                if i == 0:
                    if is_stmt_file:
                        # Snake-caseify column names
                        cols = list(map(lambda field: re.sub(r'[^a-zA-Z]+', '_', field).lower().rstrip('_'), row))
                    else:
                        cols = row
                    i += 1
                else:
                    # Combine columns and row fields into a list like PHP's array_combine()
                    transactions.append(dict(zip(cols, row)))
                    i += 1

        if is_orders_file:
            entries = self.extract_orders(transactions)
        elif is_stmt_file:
            entries = self.extract_statement(transactions)
        else:
            entries = []

        return entries

    def extract_orders(self, transactions):
        # flip into chronological order
        transactions.reverse()

        entries = []

        for tx in transactions:
            # {'date_utc': '2018-04-15 15:44:03', 'fill_date_utc': '2018-04-15 15:44:03', 'description': 'VIAETH market buy',
            # 'order_id': '3589044', 'base_asset': 'VIA', 'quote_asset': 'ETH', 'amount': '51.9600000000',
            # 'price': '0.00384300', 'comm0_amount': '0.05196', 'comm0_asset': 'VIA'}
            tx["base_asset"] = self.escape_asset_name(tx["base_asset"])
            tx["quote_asset"] = self.escape_asset_name(tx["quote_asset"])

            if len(tx["fill_date_utc"]):
                fill_date = dateutil.parser.parse(tx["fill_date_utc"]).date()
            else:
                # Happens sometimes due to binance api issue
                print("Fill date missing for entry, reverting to order placement date (date_utc): " + str(tx),
                      file=sys.stderr)
                fill_date = dateutil.parser.parse(tx["date_utc"]).date()

            side = re.search(r"(buy|sell)$", tx["description"]).group(0)

            postings = []

            if side == 'buy':
                self.push_lot({
                    "amount": D(tx["amount"]),
                    "price": D(tx["price"]),
                    "base_asset": tx["base_asset"],
                    "quote_asset": tx["quote_asset"],
                    "fill_date": fill_date
                })

                postings.append(data.Posting(
                    self.asset_account,
                    bc_amount.Amount(D(tx["amount"]), tx["base_asset"]),
                    # position.Cost(
                    #     number=D(tx["price"]),
                    #     currency=tx["quote_asset"],
                    #     date=fill_date,
                    #     label=""
                    # ),
                    None,
                    bc_amount.Amount(
                        D(tx["price"]),
                        tx["quote_asset"]
                    ), None, None,
                ))

                # Account to subtract from
                postings.append(data.Posting(
                    self.asset_account,
                    bc_amount.Amount(-D(tx["amount"]) * D(tx["price"]), tx["quote_asset"]),
                    None, None, None, None,
                ))
            else:  # sell order
                amount_obtained = D(tx["amount"]) * D(tx["price"])

                lots = self.satisfy_lots(D(tx["amount"]), tx["base_asset"], fill_date)
                for i, lot in enumerate(lots):
                    postings.append(data.Posting(
                        self.asset_account,
                        bc_amount.Amount(-lot["amount_used"], lot["base_asset"]),
                        None,
                        bc_amount.Amount(
                            D(tx["price"]),
                            tx["quote_asset"]
                        ),
                        None, None
                    ))

                # Asset posting
                postings.append(data.Posting(
                    self.asset_account,
                    bc_amount.Amount(
                        amount_obtained,
                        tx["quote_asset"]
                    ),
                    None, None, None, None
                ))

                # Push the capital we just obtained as another lot
                # Don't keep track of the effective price we bought it for (yet?)
                self.push_lot({
                    "amount": D(tx["amount"]) * D(tx["price"]),
                    "price": None,
                    "base_asset": tx["quote_asset"],
                    "quote_asset": None,
                    "fill_date": fill_date
                })

                # PnL posting - disabled, see readme
                # postings.append(data.Posting(
                #     self.pnl_account,
                #     None, None, None, None, None
                # ))

            buy_or_sell_entry = data.Transaction(
                data.new_metadata("", 0),
                fill_date, "*", "",
                tx["description"],
                data.EMPTY_SET, data.EMPTY_SET,
                postings,
            )
            entries.append(buy_or_sell_entry)

            # Commission - separate entry because BC doesn't like fees in different currencies
            postings = []

            have_fee = False
            if D(tx["comm0_amount"]) > 0:
                have_fee = True
                lots = self.satisfy_lots(D(tx["comm0_amount"]), tx["comm0_asset"], fill_date)
                for lot in lots:
                    postings.append(self.lot_to_posting(lot))
                postings.append(data.Posting(
                    self.commission_account,
                    None, None, None, None, None
                ))
            if "comm1_amount" in tx and D(tx["comm1_amount"]) > 0:
                have_fee = True
                lots = self.satisfy_lots(D(tx["comm1_amount"]), tx["comm1_asset"], fill_date)
                for lot in lots:
                    postings.append(self.lot_to_posting(lot))

            if have_fee:
                commission_entry = data.Transaction(
                    data.new_metadata("", 0),
                    fill_date, "*", "",
                    tx["description"] + " fee",
                    data.EMPTY_SET, data.EMPTY_SET,
                    postings,
                )
                entries.append(commission_entry)

        return entries

    def extract_statement(self, transactions):
        entries = []

        last_date = None

        # Conversions triggered when clicking converting "dust" (small holdings) to BNB
        exchangedToBNB = {}

        # Operations marked "The Easiest Way to Trade"
        # Presumably assets traded through the "simple view" on Binance.com
        # These are not included in Binance orders / trades CSV statements for some reason
        simpleTraded = {}

        transactions.append(None)  # Add dummy so we can clear up any txns held in dicts above

        for tx in transactions:
            # {'user_id': '13017299', 'utc_time': '2018-04-13 09:24:23', 'account': 'Spot',
            # 'operation': 'Deposit', 'coin': 'ETH', 'change': '1.50000000', 'remark': ''}

            if tx is not None:
                date = dateutil.parser.parse(tx["utc_time"]).date()
                last_date = date
                change, operation, coin, remark = itemgetter('change', 'operation', 'coin', 'remark')(tx)
                coin = self.escape_asset_name(coin)

                if operation == "The Easiest Way to Trade":
                    if D(change) >= 0:
                        simpleTraded["target"] = {"coin": coin, "amount": D(change)}
                    else:
                        simpleTraded["source"] = {"coin": coin, "amount": D(change)}
                    continue
                elif operation == "Small assets exchange BNB":
                    exchangedToBNB[coin] = change
                    continue
                # Usually freebies from Binance, see exceptions below
                elif operation == "Distribution":
                    postings = [
                        data.Posting(
                            self.asset_account,
                            bc_amount.Amount(D(change), coin),
                            None, None, None, None
                        ),
                        data.Posting(
                            "Income:Binance:Distribution" if D(change) > 0 else "Expenses:Binance:Distribution",
                            None, None, None, None, None
                        )
                    ]
                    if D(change) > 0:
                        self.push_lot({
                            "amount": D(change),
                            "price": None,
                            "base_asset": coin,
                            "quote_asset": None,
                            "fill_date": date
                        })
                    else:
                        # Negative distribution (Binance _taking_ something from you)
                        # Observed situations: coin name changes (STORM->STMX) or delistings (SALT->BUSD)
                        lots = self.satisfy_lots(D(change), coin, date)
                        for lot in lots:
                            # E.g. Assets:Binance -1 ADA {0.00042735 ETH, 2018-04-15}
                            postings.append(self.lot_to_posting(lot))

                    entry = data.Transaction(
                        data.new_metadata("", 0),
                        date, "*", "",
                        "{} {} {} {}".format(tx["account"], operation, coin, remark).strip(),
                        data.EMPTY_SET, data.EMPTY_SET,
                        postings,
                    )
                    entries.append(entry)
                elif operation == "Deposit":
                    entry = data.Transaction(
                        data.new_metadata("", 0),
                        date, "*", "",
                        "{} {} {} {}".format(tx["account"], operation, coin, remark).strip(),
                        data.EMPTY_SET, data.EMPTY_SET,
                        [
                            data.Posting(
                                self.asset_account,
                                bc_amount.Amount(D(change), coin),
                                None, None, None, None
                            ),
                            data.Posting(
                                "Equity:Unknown",
                                None, None, None, None, None
                            )
                        ],
                    )
                    entries.append(entry)
                    self.push_lot({
                        "amount": D(change),
                        "price": None,
                        "base_asset": coin,
                        "quote_asset": None,
                        "fill_date": date
                    })

            # Flush whatever we built up in previous rows
            if len(exchangedToBNB) > 0:
                postings = [
                    data.Posting(self.asset_account, None, None, None, None, None)
                ]
                for bnb_asset, bnb_amount in exchangedToBNB.items():
                    posting = data.Posting(
                        self.asset_account,
                        bc_amount.Amount(D(bnb_amount), bnb_asset),
                        None, None, None, None
                    )
                    if bnb_asset == "BNB":
                        postings.insert(1, posting)  # Insert at top for ease of reading
                        self.push_lot({
                            "amount": D(bnb_amount),
                            "price": None,
                            "base_asset": "BNB",
                            "quote_asset": None,
                            "fill_date": last_date
                        })
                    else:
                        postings.append(posting)
                entries.append(data.Transaction(
                    data.new_metadata("", 0),
                    last_date, "*", "",
                    "Small assets exchange into BNB",
                    data.EMPTY_SET, data.EMPTY_SET,
                    postings,
                ))
                # TODO: clean up any dust from self.lots
                exchangedToBNB = {}

            if len(simpleTraded) > 0:
                source, target = itemgetter('source', 'target')(simpleTraded)
                cost = abs(source["amount"]) / target["amount"]
                postings = [
                    data.Posting(
                        self.asset_account,
                        bc_amount.Amount(target["amount"], target["coin"]),
                        # position.Cost(
                        #     number=cost,
                        #     currency=source["coin"], date=last_date, label=""
                        # ),
                        None,
                        bc_amount.Amount(
                            cost,
                            source["coin"]
                        ), None, None
                    ),
                    data.Posting(
                        self.asset_account,
                        bc_amount.Amount(source["amount"], source["coin"]),
                        None, None, None, None
                    )
                ]
                entries.append(data.Transaction(
                    data.new_metadata("", 0),
                    last_date,
                    "*",
                    "",
                    "Manual exchange via simple interface",
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                ))
                self.push_lot({
                    "amount": D(target["amount"]),
                    "price": cost,
                    "base_asset": target["coin"],
                    "quote_asset": source["coin"],
                    "fill_date": last_date
                })
                simpleTraded = {}

        return entries

    def push_lot(self, lot):
        if lot["amount"] <= 0:
            raise Exception("Attempted to push negative amount to lot", lot)

        lot["amount_left"] = lot["amount"]
        self.lots.append(lot)
        # Make sure lots are always in chronological order (because we can mix statement and orders CSV files)
        self.lots.sort(key=lambda l: l["fill_date"])

    def satisfy_lots(self, amount, base_asset, fill_date):
        left_to_sell = amount
        found_lots = []

        # Note: copying self.lots with list() so we don't manipulate what we're looping over
        for i, lot in enumerate(list(self.lots)):
            if lot["base_asset"] != base_asset or lot["fill_date"] > fill_date:
                continue

            if lot["amount_left"] > left_to_sell:
                lot["amount_left"] -= left_to_sell  # TODO: check if no number issues here
                found_lots.append({**lot, "amount_used": left_to_sell})
                left_to_sell = 0
                break
            else:  # lot amount is smaller or equal to required amount
                left_to_sell -= lot["amount_left"]
                found_lots.append({**lot, "amount_used": lot["amount_left"]})
                self.lots.remove(lot)  # No longer an open lot
                if left_to_sell == 0:
                    break

        if left_to_sell > 0:
            print("Not enough capital to satisfy (usually sell) transaction of " + str(fill_date) + " "
                  + str(amount) + " " + base_asset + ", used lots: " + str(found_lots),
                  file=sys.stderr)

        return found_lots

    def lot_to_posting(self, lot, at_posting_price=None):
        # cost = None if lot["price"] is None else position.Cost(
        #     number=D(lot["price"]),
        #     currency=lot["quote_asset"],
        #     date=lot["fill_date"],
        #     label=""
        # )
        # E.g. Assets:Binance -1 ADA @ 0.00049657 ETH
        return data.Posting(
            self.asset_account,
            bc_amount.Amount(-lot["amount_used"], lot["base_asset"]),
            # cost,
            None,
            # TODO: make this work without tripping BC "Cost and price currencies must match"
            # at_posting_price,
            bc_amount.Amount(
                D(lot["price"]),
                lot["quote_asset"]
            ) if lot["price"] else None,
            None,
            None
        )

    def escape_asset_name(self, name):
        """Work around assets starting with a number. Observed asset in Binance: 1INCH"""
        if re.match(r"[0-9]", name[0]):
            return "XX" + name
        return name
