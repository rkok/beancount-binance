import Binance from 'node-binance-api';
import config from './private/binance-config.json'
import fs from 'fs'
import csv from 'csv-parser'
import {createArrayCsvWriter} from 'csv-writer'
import BigNumber from "bignumber.js";
import moment from "moment";

/**
  @return Object commissions: { ETH: 0.123 }
 */
const getOrderTradeInfoFromApi = async (symbol, orderId) => {
    // Separate API call to /myTrades to retrieve commissions and fill date
    let trades;
    try {
        trades = await binance.trades(symbol, undefined, {orderId});
    } catch (e) {
        console.error(`!! [trades] Binance API error for order ${symbol}:${orderId}: ${e}`)
        return undefined;
    }

    await new Promise(resolve => {
        setTimeout(resolve, 400); // Crude rate limiting
    });

    const times = trades.map(t => t.time)
    const firstFillAt = moment.utc(Math.min(...times));
    const lastFillAt = moment.utc(Math.max(...times));

    const commissions = {};
    trades.forEach(trade => {
        const {commission, commissionAsset} = trade;
        commissions[commissionAsset] = commissions[commissionAsset] || 0;
        commissions[commissionAsset] = new BigNumber(commissions[commissionAsset]).plus(new BigNumber(commission)).toNumber();
    });

    return { commissions, firstFillAt, lastFillAt } ;
}

const getMarketsFromApi = () => {
    return binance.exchangeInfo().then(exchangeInfo => {
        return exchangeInfo.symbols.map(info => {
            return {
                symbol: info.symbol,
                baseAsset: info.baseAsset,
                baseAssetPrecision: info.baseAssetPrecision,
                quoteAsset: info.quoteAsset,
                quoteAssetPrecision: info.quoteAssetPrecision
            }
        })
    })
}

const readBinanceCsv = (path) => {
    return new Promise(resolve => {
        const rows = [];
        fs.createReadStream(path)
            .pipe(csv())
            .on('data', (rowOrig) => {
                const row = {}
                Object.keys(rowOrig).forEach((key) => {
                    const val = rowOrig[key]
                    // ' Date(UTC)' -> 'date_utc'
                    const newKey = key.trim().toLowerCase().replace(/[^a-z0-9]/g, '_').replace(/_$/, '');
                    row[newKey] = val;
                })
                rows.push(row)
            })
            .on('end', () => resolve(rows));
    })
}

const verbose = (msg) => {
    process.stderr.write(`${msg}\n`);
}

if (!process.argv[2] || !fs.existsSync(process.argv[2])) {
    verbose("Orders CSV missing or not found");
    process.exit(1)
}
if (process.argv[3] && !fs.existsSync(process.argv[3])) {
    verbose(`Trades CSV path given, but file not found: ${process.argv[3]}`);
    process.exit(1)
}

const binance = new Binance().options({
    APIKEY: config.apiKey,
    APISECRET: config.apiSecret
});

const csvFile = process.argv[2];
const tradesCsvFile = process.argv[3];
const outFile = csvFile.replace(/\.csv$/, '') + '-processed.csv';

(async () => {
    verbose(`Fetching Binance markets`);

    const markets = await getMarketsFromApi();

    verbose(`Reading ${csvFile}`);

    const rows = await readBinanceCsv(csvFile);

    // Sometimes a trades export csv from Binance is needed too,
    // because the /myTrades API endpoint won't recognize all historical markets
    // Index+combine trades into tradesRows
    const tradeOrderRows = [];
    if (tradesCsvFile) {
        verbose(`Reading ${tradesCsvFile}`);
        const tradesRowsRaw = await readBinanceCsv(tradesCsvFile);

        const txns = {};
        // Squash trades into single transactions
        tradesRowsRaw.forEach(trade => {
            const key = trade.date_utc + trade.pair + trade.side;
            const feeSplit = trade.fee.replace(/,/g, '').match(/^([0-9\.]+)([A-Z]+)$/);
            if (!feeSplit) {
                console.error('Unexpected fee in trade row:', trade);
                process.exit(1);
            }
            const [_, commAmount, commAsset] = feeSplit;
            txns[key] = txns[key] ?? {date_utc: trade.date_utc, pair: trade.pair, side: trade.side};
            txns[key].commissions = txns[key].commissions ?? {};
            txns[key].commissions[commAsset] = new BigNumber(txns[key].commissions[commAsset] ?? 0).plus(commAmount).toNumber();
        });

        for (const txn in txns) {
            tradeOrderRows.push(txns[txn]);
        }
    }

    const outData = [];

    if (!process.argv.includes('--no-resume') && fs.existsSync(outFile)) {
        verbose(`Reading (partial) ${outFile}`);

        await new Promise(resolve => {
            fs.createReadStream(outFile)
                .pipe(csv())
                .on('data', (row) => {
                    outData.push(row);
                })
                .on('end', () => resolve());
        });
    }

    /*
    // CSV rows look like:
        date_utc: '2018-05-22 17:25:14',
        orderno: '11721995',
        pair: 'CMTETH',
        type: 'Market',
        side: 'SELL',
        order_price: '0',
        order_amount: '380.0000000000CMT',
        time: '2018-05-22 17:25:14',
        executed: '380.0000000000CMT',
        average_price: '0.00050117',
        trading_total: '0.1904446000ETH',
        status: 'FILLED'
     */

    verbose(`Processing ${rows.length - outData.length} rows` + (outData.length ? ` (skipping ${outData.length} pre-imported rows)` : ''));

    for (let i = 0; i < rows.length; i++) {
        const inRow = rows[i];

        if (outData.find(row => row.order_id === inRow.orderno)) {
            continue; // Already imported
        }

        if (!['FILLED', 'PARTIALLY_FILLED'].includes(inRow.status)) {
            if (!['CANCELED', 'UNKNOWN_STATUS'].includes(inRow.status)) {
                verbose(`!! Unknown status '${inRow.status} in row ${i}`);
            }
            continue;
        }

        verbose(`Row number ${i + 1} / ${rows.length}`);

        const orderTradeInfo = await getOrderTradeInfoFromApi(inRow.pair, inRow.orderno);
        let commRaw = orderTradeInfo?.commissions;

        if (!commRaw) {
            let found = false;
            if (tradeOrderRows.length) {
                verbose(`Using trades sheet for commission`);
                const matchingRow = tradeOrderRows.find(t => t.date_utc === inRow.date_utc && t.pair === inRow.pair && t.side === inRow.side);
                if (matchingRow) {
                    commRaw = matchingRow.commissions;
                    found = true;
                } else {
                    verbose(`Order not found in trades sheet:`, inRow);
                }
            }
            if (!found) {
                console.error(`Having to skip order ${inRow.orderno} due to unknown Binance API error`);
                continue;
            }
        }

        let market = markets.find(m => m.symbol === inRow.pair);
        if (!market) {
            // Crude workaround for some deprecated markets
            const quotes = ['ETH', 'BTC', 'USDT', 'EUR', 'BNB'];
            quotes.forEach(quote => {
                if (inRow.pair.match(new RegExp(quote + '$'))) {
                    market = {baseAsset: inRow.pair.substring(0, inRow.pair.length - quote.length), quoteAsset: quote}
                    return false; // Break
                }
            })
            if (!market) {
                console.error(`Having to skip order ${inRow.orderno} due to unknown market: ${inRow.pair}`);
                continue;
            }
        }

        const commissions = {};
        Object.keys(commRaw).forEach((asset, i) => {
            commissions[`comm${i}_amount`] = commRaw[asset];
            commissions[`comm${i}_asset`] = asset;
        });

        if (orderTradeInfo?.firstFillAt.isBefore(orderTradeInfo?.lastFillAt.clone().subtract(1, 'day'))) {
            console.error(`More than 1d between first/last fill dates!`, orderTradeInfo.firstFillAt.toISOString(), orderTradeInfo.lastFillAt.toISOString());
        }

        outData.push({
            date_utc: inRow.date_utc,
            fill_date_utc: orderTradeInfo?.firstFillAt.format('YYYY-MM-DD HH:mm:ss') ?? '',
            description: `${inRow.pair} ${inRow.type.toLowerCase()} ${inRow.side.toLowerCase()}`, // "GMTETH market sell"
            order_id: inRow.orderno,
            base_asset: market.baseAsset,
            quote_asset: market.quoteAsset,
            amount: inRow.executed.replace(/,/g, '').replace(/[A-Z]+[0-9]*$/, ''), // "0.001BTC" or "1C98" (ugh)
            price: inRow.average_price.replace(/,/g, ''),
            ...commissions
        });

        outData.sort((a, b) => {
            return a.date_utc > b.date_utc ? -1 : 1 // DESC
        })

        // Write on every row so we don't lose progress if there is a screwup along the way
        const csvWriter = createArrayCsvWriter({
            path: outFile, header: [
                'date_utc', 'fill_date_utc', 'description', 'order_id', 'base_asset', 'quote_asset',
                'amount', 'price', 'comm0_amount', 'comm0_asset', 'comm1_amount', 'comm1_asset'
            ]
        });
        await csvWriter.writeRecords(outData.map(row => Object.values(row)));
    }

    verbose('Done');

    process.exit(0)
})();