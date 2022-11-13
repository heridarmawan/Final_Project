select bc.country_customer,sum(bt.amount_transaction) as jml
from bigdata_transaction bt join bigdata_customer bc  on bt.id_customer=bc.id_customer
group by bt.id_customer,bc.country_customer
