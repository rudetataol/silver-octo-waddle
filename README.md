Таня, [28.03.2023 20:58]
SELECT *
FROM dex.trades
WHERE blockchain = 'polygon'
AND block_date >= TIMESTAMP '2023-01-03'
AND (token_bought_address = 0xc2a45fe7d40bcac8369371b08419ddafd3131b4a OR token_sold_address = 0xc2a45fe7d40bcac8369371b08419ddafd3131b4a)
ORDER BY block_time DESC


Select
block_time, tx_hash, evt_index, amount_usd
from dex.trades
where project = 'uniswap'
order by 4 desc
limit 20





select date_trunc('day', block_time) as day, count(*) as txs,
sum(number_of_items) as items,
sum(usd_amount) as volume 
from seaport."view_transactions"
group by day

Таня, [28.03.2023 23:37]
with 
address(address) as (
    values
    (0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2), 
    (0xc098b2a3aa256d2140208c3de6543aaef5cd3a94)
), token_price_dec as (
    select date_trunc('day', minute) as time, symbol, contract_address, decimals, avg(price) as avg_price
    from prices.usd
    where minute >= cast('2022-11-06' as TIMESTAMP) and minute < cast('2022-11-07' as TIMESTAMP)
        and blockchain = 'ethereum'
        and contract_address = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
        and symbol = 'WETH'
    group by 1, 2, 3, 4
), eth_in as (
    select date_trunc('day', block_time) as time, 'in' as category, sum( cast(value as double) / pow(10, p.decimals)) as raw_amt, sum( cast(value as double) / pow(10, p.decimals) * p.avg_price) as usd_amt
    from ethereum.traces t
    left join token_price_dec p on p.time = date_trunc('day', block_time)
    where t.block_time >= cast('2022-11-06' as TIMESTAMP) and t.block_time < cast('2022-11-07' as TIMESTAMP)
        and t."to" in (select address from address) 
    group by 1, 2
), eth_out as (
    select date_trunc('day', block_time) as time, 'out' as category, -1 * sum( cast(value as double) / pow(10, p.decimals)) as raw_amt, -1 * sum( cast(value as double) / pow(10, p.decimals) * p.avg_price) as usd_amt
    from ethereum.traces t
    left join token_price_dec p on date_trunc('day', block_time) = p.time
    where t.block_time >= cast('2022-11-06' as TIMESTAMP) and t.block_time < cast('2022-11-07' as TIMESTAMP)
        and t."from" in (select address from address) 
    group by 1, 2
)
select sum(usd_amt) as usd_netflow
from (
    select time, category, raw_amt, usd_amt
    from eth_in
    union all 
    select time, category, raw_amt, usd_amt
    from eth_out
)
