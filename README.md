Binance importer for Beancount
==============================

Prepares a CSV using the Binance API and generates transactions using that CSV and additional other CSVs.

Incomplete, messy and the result of a lot of trial and error.

Creates transactions for:

- Spot orders
- Spot fees in multiple currencies (happens sometimes if you enable fees in BNB but don't have enough BNB)
- Deposits
- Trades made using Binance "simple view"
- Dust exchanges to BNB
- Distributions by Binance

Does NOT create transactions for:

- __Withdrawals__ <-- do these manually

Tracks ["lots"](https://beancount.github.io/docs/trading_with_beancount.html#trade-lots) FIFO style internally,
in order to apply a cost to each sell. Feature is incomplete, see "Known issues" below.

## Preparation

1. Get [NodeJS](https://nodejs.org) v14 or later
2. Run `npm install`
3. Create a file `private/binance-config.json` containing your Binance API key+secret:
   ```json
   {
     "apiKey": "faffaffaffaffaffaf",
     "apiSecret": "faffaffaffaffaffaf"
   }
   ```
4. Create a Beancount importer config file that looks like this:
    ```python
    #!/usr/bin/env python3
    # config.py
    import os, sys
    
    sys.path.insert(0, os.path.abspath("."))
    import bc_binance.importer as binance
    
    CONFIG = [
        binance.Importer(
            asset_account="Assets:Binance",
            commission_account="Expenses:Fees:Commission:Binance"
        )
    ]
    ```

## Usage

1. In Binance, export everything you can in as many steps as necessary:  
   - Orders > Spot Order > Order History > Export > Beyond 6 months (custom)    
     Note: MUST select Custom, otherwise format will be incorrect  
     Save these files as `download/bina-XXXX_XX_XX-XXXX_XX_XX.csv`
   - Same for Orders > Spot Order > Trade History > ...              <-- possibly only necessary for pre-2018 orders with markets the API doesn't recognize  
     Save these files as `download/binatrades-XXXX_XX_XX-XXXX_XX_XX.csv`
   - Wallet > Transaction History > Generate All Statements  
     Save these files as `download/0binastmt-XXXX_XX_XX-XXXX_XX_XX.csv`
   
   Keep these files in your download directory even after you're done - full history is necessary to keep track of lots


2. Generate `bina-XXXX_XX_XX-XXXX_XX_XX-processed.csv` files using the Node script:  
   USAGE:   `node preprocess-bina-file.mjs orders.csv [trades.csv]`  
   EXAMPLE: `node preprocess-bina-file.mjs download/bina-XXXX_XX_XX-XXXX_XX_XX.csv download/binatrades-XXXX_XX_XX-XXXX_XX_XX.csv`

3. Run beancount:
   ```bash
   bean-extract config.py .
   ```
   
## Quirks

- Some cryptocurrency symbols start with a digit, which is illegal in Beancount. Example: "1INCH".
  For this reason, we prefix such symbols with "XX".
- Half of the project is in NodeJS instead of Python -- 
  yeah, it seemed more convenient at the time

## Known issues

### Cost disabled

Assets:Binance  1 ATOM **{2.40473553 USDT, 2020-04-16}** <-- this part was disabled

Cause #1: a buy order failing with ["No position matches"](https://groups.google.com/g/beancount/c/Qvbcq-Sk5NY/m/evxSnb6nAQAJ)
even though there was a valid position in the balances.  
Something to do with a posting being marked as a reduction though it should be an augmentation.  
See beancount core: `parser/booking_full.py`  
Possible workaround: make it possible to force a posting as being an augmentation?

Cause #2: Beancount refusing to 'grab' from a lot that was created using a different currency.  
Error message: "Cost and price currencies must match: ETH != BTC"  
Example:
```
2018-04-24 * "STORMBTC market sell"
  Assets:Binance        -155.619 STORM {0.00000641 BTC, 2018-04-24} @ 0.00000692 BTC
  Assets:Binance        -224.381 STORM {0.00008888 ETH, 2018-04-24} @ 0.00000692 BTC  <-- CONFLICTING TX
  Assets:Binance      0.00262960 BTC
```

### PnL disabled

Cause: PnL catches profit from other currencies  
E.g. ETH bought for USD, when sold for EUR, will accumulate in PnL too

### Currency rename handling

STORM was renamed to STMX over 2020-06-09 and 2020-06-10 leading to confusing 'Distribution' statements  
Has to be fixed by hand in the generated ledger

## Thanks

Martin Blais and the other Beancount contributors

## No thanks

Binance, for their incredibly annoying and buggy export mechanism