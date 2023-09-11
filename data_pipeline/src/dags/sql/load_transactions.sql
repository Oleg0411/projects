SELECT operation_id, account_number_from, account_number_to, currency_code,
    country, status, transaction_type, amount, transaction_dt
FROM public.transactions t
WHERE t.transaction_dt::date = '{execution_date}'
    and t.status = 'done'
    and t.account_number_from > 0
    and t.account_number_to > 0
    and t.transaction_type in ('c2a_incoming',
                               'c2b_partner_incoming',
                               'sbp_incoming',
                               'sbp_outgoing',
                               'transfer_incoming',
                               'transfer_outgoing')