SELECT
    c.id_cliente,
    c.nome,
    c.cpf,
    c.email,
    c.data_nascimento,
    c.status,
    c.data_evento,

    concat_ws(', ',
        CASE 
            WHEN c.id_cliente IS NULL
            THEN 'id_cliente  nulo_vazio'
        END,
        CASE 
            WHEN c.nome IS NULL OR TRIM(c.nome) = '' OR LOWER(TRIM(c.nome)) IN ('nan', 'null')
            THEN 'nome  nulo_vazio'
        END,
        CASE 
            WHEN c.cpf NOT RLIKE '^\\d{3}\\.\\d{3}\\.\\d{3}-\\d{2}$'
            THEN 'cpf formato_invalido'
        END,
        CASE 
            WHEN c.email NOT RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
            THEN 'email formato_invalido'
        END,
        CASE 
            WHEN try_cast(c.data_nascimento AS DATE) IS NULL
            THEN 'data_nascimento invalida'
        END,
        CASE 
            WHEN c.status NOT IN ('ativo', 'inativo', 'suspenso')
            THEN 'status formato_invalido'
        END,
        CASE 
            WHEN try_to_timestamp(c.data_evento, 'yyyy-MM-dd HH:mm:ss') IS NULL
            THEN 'data_evento formato_invalido'
        END
    ) AS erro

FROM clientes c