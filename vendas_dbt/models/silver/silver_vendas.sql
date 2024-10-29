{{ config(materialized='view') }}

WITH cleaned_data AS(
    SELECT
        email,
        DATE(data) AS data,
        round(cast(valor as decimal(10,2)),2) as valor,
        quantidade,
        LOWER(produto) as produto
    FROM
        {{ ref('bronze_vendas') }}
    WHERE
        valor > 1000
        AND valor < 8000
        AND data <= CURRENT_DATE
)

SELECT * FROM cleaned_data