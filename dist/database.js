/**
 *
 * @author nery
 * @version 0.2.20180116
 *
 */

 /** @ignore */
let Statement = Java.type("java.sql.Statement")
let Timestamp = Java.type("java.sql.Timestamp")
var DataSource = Java.type("org.apache.tomcat.jdbc.pool.DataSource")

let config = getConfig()

config.dsm = config.dsm || {}

const sqlInjectionError = {
    error: true,
    message: "Attempt sql injection!"
}


function createDbInstance(options) {
    options.logFunction = options.logFunction || function(dbFunctionName, statementMethodName, sql) { }

    let ds = createDataSource(options)
    let ctx = {
        stringDelimiter: options.stringDelimiter || "'",
        logFunction: options.logFunction
    }

    return {
        "getInfoColumns": getInfoColumns.bind(ctx, ds),

        "insert": tableInsert.bind(ctx, ds),

        "select": sqlSelect.bind(ctx, ds),

        "update": tableUpdate.bind(ctx, ds),

        "delete": tableDelete.bind(ctx, ds),

        "execute": sqlExecute.bind(ctx, ds)
    }

}


function getInfoColumns(ds, table) {
    let cnx = getConnection(ds)
    let databaseMetaData = cnx.getMetaData()
    let infosCols = databaseMetaData.getColumns(null,null, table, null)
    let cols = []

    while(infosCols.next()) {
        let column = {}

        column.name = infosCols.getString("COLUMN_NAME")
        column.dataType = infosCols.getString("DATA_TYPE")
        column.size = infosCols.getString("COLUMN_SIZE")
        column.decimalDigits = infosCols.getString("DECIMAL_DIGITS")
        column.isNullable = infosCols.getString("IS_NULLABLE")
        column.isAutoIncrment = infosCols.getString("IS_AUTOINCREMENT")
        column.ordinalPosition = infosCols.getString("ORDINAL_POSITION")
        column.isGeneratedColumn = infosCols.getString("IS_GENERATEDCOLUMN")

        cols.push(column)
        // print(JSON.stringify(column))
    }

    cnx.close()
    conn = null

    return cols
}


function createDataSource(options) {
    let urlConnection = options.urlConnection

    if (config.dsm[urlConnection]) {
        return config.dsm[urlConnection]
    }

    options.logFunction("createDataSource", "DataSource", urlConnection)

    let ds = new DataSource()
    let cfg = Object.assign({
        "initialSize": 5,
        "maxActive": 15,
        "maxIdle": 7,
        "minIdle": 3,
        "userName": "",
        "password": ""
    }, options)

    ds.setDriverClassName(cfg.driverClassName)
    ds.setUrl(cfg.urlConnection)
    ds.setUsername(cfg.userName)
    ds.setPassword(cfg.password)
    ds.setInitialSize(cfg.initialSize)
    ds.setMaxActive(cfg.maxActive)
    ds.setMaxIdle(cfg.maxIdle)
    ds.setMinIdle(cfg.minIdle)

    config.dsm[urlConnection] = ds

    return ds
}


/**
 * Retorna um objeto Connection que representa a conexão com o banco de dados. Esta API é exclusiva para uso
 * interno do Thrust.
 * @param {boolean} autoCommit - Utilizado para definir se a conexão com o banco deve fazer *commit*
 * a cada execução de uma commando SQL.
 * @returns {Connection}
 */
function getConnection(ds, autoCommit) {
    let connection = ds.getConnection()

    connection.setAutoCommit((autoCommit !== undefined) ? autoCommit : true)

    return connection
}


function hasSqlInject(sql) {
    var testSqlInject = sql.match(/[\t\r\n]|(--[^\r\n]*)|(\/\*[\w\W]*?(?=\*)\*\/)/gi)

    return (testSqlInject != null) ? true : false
}


function sqlInsert(ds, sql, returnGeneratedKeys) {
    let cnx, stmt, rsk, rows

    if (hasSqlInject(sql)) {
        return sqlInjectionError
    }

    cnx = this.connection || getConnection(ds)
    stmt = cnx.prepareStatement(sql, (returnGeneratedKeys)
        ? Statement.RETURN_GENERATED_KEYS
        : Statement.NO_GENERATED_KEYS)
    this.logFunction("execute", "executeUpdate", sql)
    stmt.executeUpdate()
    rsk = stmt.getGeneratedKeys()
    rows = []

    while (rsk.next()) {
        rows.push(rsk.getObject(1))
    }

    stmt.close()
    stmt = null

    if (!this.connection) {
        cnx.close()
        cnx = null
    }

    return {
        error: false,
        keys: rows
    }
}


function sqlSelect(ds, sql, data) {
    let cnx, stmt, rs, rsmd, numColumns, result, params

    if (hasSqlInject(sql)) {
        return sqlInjectionError
    }

    cnx = this.connection || getConnection(ds)
    params = sql.match(/:\w+/g)
    sql = sql.replace(/(:\w+)/g, "?")
    stmt = cnx.prepareStatement(sql)

    if (data && data.constructor.name === "Object") {
        params = params.map(function (param) {
            return param.slice(1)
        })
        for (let name in data) {
            let value = data[name]
            let col = params.indexOf(name) + 1

            switch (value.constructor.name) {
                case "String":
                    stmt.setString(col, value)
                    break

                case "Number":
                    if (Math.floor(value) === value) {
                        stmt.setInt(col, value)
                    } else {
                        stmt.setFloat(col, value)
                    }
                    break

                default:
                    stmt.setObject(col, value)
                    break
            }

            col++
        }
    }

    this.logFunction("execute", "executeQuery", sql)
    rs = stmt.executeQuery()

    result = fetchRows(rs)

    stmt.close()
    stmt = null

    if (!this.connection) {
        cnx.close()
        cnx = null
    }

    return result
}


function sqlExecute(ds, sql, dataOrReturnGeneratedKeys) {
    let cnx, stmt, result
    let sql_select = sqlSelect.bind(this, ds)
    let sql_insert = sqlInsert.bind(this, ds)

    if (hasSqlInject(sql)) {
        return sqlInjectionError
    }

    if (sql.substring(0,6).toUpperCase() === "SELECT") {
        return sql_select(sql, dataOrReturnGeneratedKeys)
    } else if (sql.substring(0,6).toUpperCase() === "INSERT") {
        return sql_insert(sql, dataOrReturnGeneratedKeys)
    }

    cnx = this.connection || getConnection(ds)
    stmt = cnx.prepareStatement(sql.trim())
    this.logFunction("execute", "executeUpdate", sql)
    result = stmt.executeUpdate()

    stmt.close()
    stmt = null

    if (!this.connection) {
        cnx.close()
        cnx = null
    }

    return {
        error: false,
        affectedRows: result
    }
}


function fetchRows(rs) {
    let rsmd = rs.getMetaData()
    let numColumns = rsmd.getColumnCount()
    let columns = []
    let types = []
    let rows = []

    for (let cl = 1; cl < numColumns + 1; cl++) {
        columns[cl] = rsmd.getColumnLabel(cl)
        types[cl] = rsmd.getColumnType(cl)
    }

    while (rs.next()) {
        let row = {}

        for (let nc = 1; nc < numColumns + 1; nc++) {
            let value

            if (types[nc] === java.sql.Types.BINARY) {
                value = rs.getBytes(nc)
            } else {
                value = rs.getObject(nc)
            }

            if (rs.wasNull()) {
                row[columns[nc]] = null
            } else if ([91, 92, 93].indexOf(types[nc]) >= 0) {
                row[columns[nc]] = value.toString()
            } else if (types[nc] == java.sql.Types.OTHER) { // json in PostgreSQL
                try {
                    row[columns[nc]] = JSON.parse(value)
                } catch(error) {
                    row[columns[nc]] = value
                }
            } else {
                row[columns[nc]] = value
            }
        }

        rows.push(row)
    }

    return rows
}


/**
 * Insere um ou mais objetos na tabela.
 * @param {String} table - Nome da tabela
 * @param {Array|Object} itens - Array com objetos a serem inseridos na tabela,
 * ou objeto único a ser inserido.
 * @return {Array} Retorna um Array com os ID's (chaves) dos itens inseridos.
 */
function tableInsert(ds, table, itens) {
    let logFunction = this.logFunction
    let sdel = this.stringDelimiter
    let cnx = this.connection || getConnection(ds)
    let keys = []
    let stmt
    let affected

    function buildSqlCommand(reg) {
        let vrg = ""
        let cols = ""
        let values = ""
        let value

        for (let key in reg) {
            val = reg[key]
            cols += vrg + key
            values += (val.constructor.name === "Number")
                ? (vrg + val)
                : (vrg + sdel + val + sdel)

            vrg = ","
        }

        // print( "INSERT INTO " + table + " (" + cols + ") " + "VALUES (" + values + ") " )
        return "INSERT INTO " + table + " (" + cols + ") " + "VALUES (" + values + ") "
    }

    if (itens.constructor.name == "Array") {
        stmt = cnx.createStatement()

        itens.forEach(function (reg, idx) {
            let sql = buildSqlCommand(reg)

            if (hasSqlInject(sql)) {
                return sqlInjectionError
            }

            logFunction("insert", "addBatch", sql)
            stmt.addBatch(sql)
        })

        logFunction("insert", "executeBatch", "")
        affected = stmt.executeBatch()
    } else {
        let sql = buildSqlCommand(itens)

        if (hasSqlInject(sql)) {
            return sqlInjectionError
        }

        stmt = cnx.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
        logFunction("insert", "executeUpdate", sql)
        affected = stmt.executeUpdate()
    }

    let rsKeys = stmt.getGeneratedKeys()

    while (rsKeys.next()) {
        keys.push(rsKeys.getObject(1))
    }

    /* se a transação não existia e foi criada, precisa ser fechada para retornar ao pool */
    if (!this.connection) {
        cnx.close()
        conn = null
    }

    return {
        error: false,
        keys: keys,
        affectedRows: affected
    }
}


/**
 * Atualiza um ou mais dados da tabela no banco.
 * @param {String} table - Nome da tabela
 * @param {Object} row - Dados das colunas a serem atualizadas no banco de dados.
 * @param {Object} whereCondition - Condição das colunas a serem atualizadas no banco de dados.
 * @returns {Object} Objeto que informa o status da execução do comando e a quantidade de
 * linhas afetadas.
 */
function tableUpdate(ds, table, row, whereCondition) {
    let sdel = this.stringDelimiter
    let values = ""
    let where = ""
    let vrg = ""
    let and = ""

    for (let col in row) {
        let val = row[col]

        values += vrg + col + " = "
        values += (val.constructor.name === "Number")
            ? val
            : (sdel + val + sdel)

        vrg = ", "
    }

    if (whereCondition) {
        for (let wkey in whereCondition) {
            let val = whereCondition[wkey]

            where += and + wkey + " = "
            where += (val.constructor.name === "Number")
                ? val
                : (sdel + val + sdel)

            and = " AND "
        }
    }

    let sql = "UPDATE " + table + " SET " + values + ((whereCondition) ? " WHERE " + where : "")

    if (hasSqlInject(sql)) {
        return sqlInjectionError
    }

    let cnx = this.connection || getConnection(ds)
    let stmt = cnx.prepareStatement(sql)
    this.logFunction("update", "executeUpdate", sql)
    let result = stmt.executeUpdate()

    /* se a transação não existia e foi criada, precisa ser fechada para retornar ao pool */
    if (!this.connection) {
        cnx.close()
        conn = null
    }

    return {
        error: false,
        affectedRows: result
    }
}


/**
 * apaga um ou mais dados da tabela no banco.
 * @param {String} table - Nome da tabela
 * @param {Object} whereCondition - Condição das colunas a serem apagadas no banco de dados.
 * @returns {Object} Objeto que informa o status da execução do comando e a quantidade de
 * linhas afetadas.
 */
function tableDelete(ds, table, whereCondition) {
    let sdel = this.stringDelimiter
    let where = ""
    let vrg = ""
    let and = ""
    let result

    if (whereCondition) {
        for (let wkey in whereCondition) {
            let val = whereCondition[wkey]

            where += and + wkey + " = "
            where += (val.constructor.name === "Number")
                ? val
                : (sdel + val + sdel)

            and = " AND "
        }
    }

    let sql = "DELETE FROM " + table + ((whereCondition) ? " WHERE " + where : "")

    if (hasSqlInject(sql)) {
        return sqlInjectionError
    }

    let cnx = this.connection || getConnection(ds)
    let stmt = cnx.prepareStatement(sql)

    this.logFunction("delete", "executeUpdate", sql)
    result = stmt.executeUpdate()

    /* se a transação não existia e foi criada, precisa ser fechada para retornar ao pool */
    if (!this.connection) {
        cnx.close()
        conn = null
    }

    return {
        error: false,
        affectedRows: result
    }
}


/**
 * Executa uma função dentro de uma única transação.
 * @param {Function} fncScript - função com vários acessos a banco de dados que recebe como parâmetros
 * um objecto com um único método *execute* equivalente ao `db.execute` e um objeto *context*.
 * @param {Object} context - um objeto que será passado como segundo parâmetro da função *fncScript*.
 * @returns {Object}
 */
function executeInSingleTransaction(ds, fncScript, context) {
    let rs
    let cnx = getConnection(ds)
    let ctx = {
        connection: cnx
    }

    var update = function (table, row, where) {
        return db.update(table, row, where, connection)
    }

    var deleteFnc = function (table, row) {
        return db["delete"](table, row, connection)
    }

    var deleteByExample = function (table, row) {
        return db.deleteByExample(table, row, connection)
    }

    try {
        cnx.setAutoCommit(false)

        rs = {
            error: false,

            result: fncScript({
                "execute": sqlExecute.bind(ctx, ds),
                "insert": tableInsert.bind(ctx, ds),
                "delete": sqlExecute.bind(ctx, ds),
                "update": sqlExecute.bind(ctx, ds)/* ,
                deleteByExample: deleteByExample */
            }, context)
        }

        cnx.commit()
    } catch (ex) {
        // print("Exception => ", ex)
        cnx.rollback()

        rs = {
            error: true,
            execption: ex
        }
    } finally {
        cnx.close()
        cnx = null
    }

    return rs
}


/**
 * @param {FileInputStream} fis
 * @param {int} size
 */
function Blob(fis, size) {
    this.fis = fis
    this.size = size
}


exports = {
    createDbInstance: createDbInstance
}
