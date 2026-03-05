SELECT
    e.id_endereco,
    e.id_cliente,
    e.cep,
    e.logradouro,
    e.numero,
    e.complemento,
    e.bairro,
    e.cidade,
    e.estado,
    e.data_evento,

    concat_ws(', ',
        CASE
            WHEN e.id_cliente IS NULL OR e.id_endereco IS NULL OR e.cep IS NULL OR TRIM(e.cep) = ''
              OR e.logradouro IS NULL OR TRIM(e.logradouro) = '' OR LOWER(TRIM(e.logradouro)) IN ('nan', 'null')
              OR e.numero IS NULL OR TRIM(e.numero) = ''
              OR e.bairro IS NULL OR TRIM(e.bairro) = ''
              OR e.cidade IS NULL OR TRIM(e.cidade) = ''
              OR e.estado IS NULL OR TRIM(e.estado) = ''
              OR e.data_evento IS NULL
            THEN 'campos nulo_ou_vazio'
        END,
        CASE
            WHEN e.id_cliente NOT IN (SELECT id_cliente FROM clientes WHERE id_cliente IS NOT NULL)
            THEN 'id_cliente nao_existe'
        END,
        CASE
            WHEN CAST(e.cep AS STRING) NOT RLIKE '^\\d{5}-\\d{3}$'
            THEN 'cep formato_invalido'
        END,
        CASE
            WHEN e.estado NOT RLIKE '^[A-Za-z]{2}$'
            THEN 'estado formato_invalido'
        END,
        CASE 
            WHEN try_to_timestamp(e.data_evento, 'yyyy-MM-dd HH:mm:ss') IS NULL
            THEN 'data_evento formato_invalido'
        END
    ) AS erro

FROM enderecos e