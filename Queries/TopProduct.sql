select product_transaction, sum(amount_transaction) as jml  from bigdata_transaction group by product_transaction 
order by jml desc 

