/**
 *
 * @author nery
 * @version 0.2.20180119
 *
 */

let Types = Java.type('java.sql.Types')
let Statement = Java.type('java.sql.Statement')
var DataSource = Java.type('org.apache.tomcat.jdbc.pool.DataSource')

let config = getConfig()

config.dsm = config.dsm || {}

const sqlInjectionError = {
  error: true,
  message: 'Attempt sql injection!'
}

function createDbInstance(options) {
  options.logFunction = options.logFunction || function (dbFunctionName, statementMethodName, sql) { }

  let ds = createDataSource(options)
  let ctx = {
    stringDelimiter: options.stringDelimiter || "'",
    logFunction: options.logFunction
  }

  return {
    getInfoColumns: getInfoColumns.bind(ctx, ds),

    insert: tableInsert.bind(ctx, ds),

    select: sqlSelect.bind(ctx, ds),

    update: tableUpdate.bind(ctx, ds),

    delete: tableDelete.bind(ctx, ds),

    execute: sqlExecute.bind(ctx, ds),

    executeInSingleTransaction: executeInSingleTransaction.bind(ctx, ds)
  }
}

function getInfoColumns(ds, table) {
  let cnx = getConnection(ds)
  let databaseMetaData = cnx.getMetaData()
  let infosCols = databaseMetaData.getColumns(null, null, table, null)
  let cols = []

  while (infosCols.next()) {
    let column = {}

    column.name = infosCols.getString('COLUMN_NAME')
    column.dataType = infosCols.getString('DATA_TYPE')
    column.size = infosCols.getString('COLUMN_SIZE')
    column.decimalDigits = infosCols.getString('DECIMAL_DIGITS')
    column.isNullable = infosCols.getString('IS_NULLABLE')
    column.isAutoIncrment = infosCols.getString('IS_AUTOINCREMENT')
    column.ordinalPosition = infosCols.getString('ORDINAL_POSITION')
    column.isGeneratedColumn = infosCols.getString('IS_GENERATEDCOLUMN')

    cols.push(column)
    // print(JSON.stringify(column))
  }

  cnx.close()
  cnx = null

  return cols
}

function createDataSource(options) {
  let urlConnection = options.urlConnection

  if (config.dsm[urlConnection]) {
    return config.dsm[urlConnection]
  }

  options.logFunction('createDataSource', 'DataSource', urlConnection)

  let ds = new DataSource()
  let cfg = Object.assign({
    'initialSize': 5,
    'maxActive': 15,
    'maxIdle': 7,
    'minIdle': 3,
    'userName': '',
    'password': ''
  }, options)

  ds.setDriverClassName(cfg.driverClassName)
  ds.setUrl(cfg.urlConnection)
  ds.setUsername(cfg.userName)
  ds.setInitialSize(cfg.initialSize)
  ds.setMaxActive(cfg.maxActive)
  ds.setMaxIdle(cfg.maxIdle)
  ds.setMinIdle(cfg.minIdle)

  if (!cfg.decryptClassName) {
    ds.setPassword(cfg.password)
  } else {
    let DecryptClass = Java.type(cfg.decryptClassName)
    let descryptInstance = new DecryptClass()
    ds.setPassword(descryptInstance.decrypt(cfg.password))
  }

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

  return (testSqlInject != null)
}

function processNamedParameters (sql) {
  var params = []

  sql = sql.replace(/([\w:])?(:\w+)/g, function ($0, $1) {
    !$1 && params.push($0.substring(1))
    return ($1 ? $0 : '?')
  })

  return {
    sql: sql,
    params: params
  }
}

function prepareStatement(cnx, sql, data, returnGeneratedKeys) {
  let stmt
  let params

  if (data) {
    var result = processNamedParameters(sql)

    sql = result.sql
    params = result.params
  }

  sql = sql.replace(/(:\w+)/g, '?')
  stmt = (returnGeneratedKeys)
    ? cnx.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
    : cnx.prepareStatement(sql)

  if (params && data && data.constructor.name === 'Object') {
    for (let index in params) {
      index = Number(index)

      let name = params[index]

      if (!data.hasOwnProperty(name)) {
        throw new Error('Error while processing a query prameter. Parameter \'' + name + '\' don\'t exists on the parameters object')
      }

      let value = data[name]
      let col = index + 1

      if (value === undefined || value === null) {
        stmt.setObject(col, null)
      } else {
        switch (value.constructor.name) {
          case 'String':
            stmt.setString(col, value)
            break

          case 'Number':
            if (Math.floor(value) === value) {
              stmt.setLong(col, value)
            } else {
              stmt.setDouble(col, value)
            }
            break

          case 'Boolean':
            stmt.setBoolean(col, value)
            break

          case 'Date':
            stmt.setTimestamp(col, new java.sql.Timestamp(value.getTime()))
            break

          case 'Blob':
            stmt.setBinaryStream(col, value.fis, value.size)
            break

          default:
            stmt.setObject(col, value)
            break
        }
      }
    }
  }

  return stmt
}

function sqlInsert(ds, sql, data, returnGeneratedKeys) {
  let cnx, stmt, rsk, rows

  if (hasSqlInject(sql)) {
    return sqlInjectionError
  }

  cnx = this.connection || getConnection(ds)
  // stmt = cnx.prepareStatement(sql, (returnGeneratedKeys)
  //     ? Statement.RETURN_GENERATED_KEYS
  //     : Statement.NO_GENERATED_KEYS)
  stmt = prepareStatement(cnx, sql, data, returnGeneratedKeys)
  this.logFunction('execute', 'executeUpdate', sql)
  stmt.executeUpdate()
  rsk = stmt.getGeneratedKeys()
  rows = []

  while (rsk && rsk.next()) {
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
  let cnx, stmt, rs, result

  if (hasSqlInject(sql)) {
    return sqlInjectionError
  }

  cnx = this.connection || getConnection(ds)
  stmt = prepareStatement(cnx, sql, data)

  this.logFunction('execute', 'executeQuery', sql)
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

function sqlExecute(ds, sql, data, returnGeneratedKeys) {
  let cnx, stmt, result
  let sqlSelectCtx = sqlSelect.bind(this, ds)
  let sqlInsertCtx = sqlInsert.bind(this, ds)

  if (hasSqlInject(sql)) {
    return sqlInjectionError
  }

  if (sql.substring(0, 6).toUpperCase() === 'SELECT') {
    return sqlSelectCtx(sql, data)
  } else if (sql.substring(0, 6).toUpperCase() === 'INSERT') {
    return sqlInsertCtx(sql, data, returnGeneratedKeys)
  }

  cnx = this.connection || getConnection(ds)
  // stmt = cnx.prepareStatement(sql.trim())
  stmt = prepareStatement(cnx, sql.trim(), data)
  this.logFunction('execute', 'executeUpdate', sql)
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
    // columns[cl] = rsmd.getColumnLabel(cl)
    columns[cl] = rsmd.getColumnName(cl)
    types[cl] = rsmd.getColumnType(cl)
  }

  while (rs.next()) {
    let row = {}

    for (let nc = 1; nc < numColumns + 1; nc++) {
      let value

      if (types[nc] === Types.BINARY) {
        value = rs.getBytes(nc)
      } else {
        value = rs.getObject(nc)
      }

      if (rs.wasNull()) {
        row[columns[nc]] = null
      } else if ([91, 92, 93].indexOf(types[nc]) >= 0) {
        row[columns[nc]] = value.toString()
      } else if (types[nc] === Types.OTHER) { // json in PostgreSQL
        try {
          row[columns[nc]] = JSON.parse(value)
        } catch (error) {
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
    let vrg = ''
    let cols = ''
    let values = ''
    let value

    for (let key in reg) {
      value = reg[key]
      cols += vrg + '"' + key + '"'
      values += (value.constructor.name === 'Number')
        ? (vrg + value)
        : (vrg + sdel + value + sdel)

      vrg = ','
    }

    // print( "INSERT INTO " + table + " (" + cols + ") " + "VALUES (" + values + ") " )
    return 'INSERT INTO "' + table + '" (' + cols + ') ' + 'VALUES (' + values + ') '
  }

  if (itens.constructor.name === 'Array') {
    stmt = cnx.createStatement()

    itens.forEach(function (reg, idx) {
      let sql = buildSqlCommand(reg)

      if (hasSqlInject(sql)) {
        return sqlInjectionError
      }

      logFunction('insert', 'addBatch', sql)
      stmt.addBatch(sql)
    })

    logFunction('insert', 'executeBatch', '')
    affected = stmt.executeBatch()
  } else {
    let sql = buildSqlCommand(itens)

    if (hasSqlInject(sql)) {
      return sqlInjectionError
    }

    stmt = cnx.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
    logFunction('insert', 'executeUpdate', sql)
    affected = stmt.executeUpdate()
  }

  let rsKeys = stmt.getGeneratedKeys()

  while (rsKeys.next()) {
    keys.push(rsKeys.getObject(1))
  }

  /* se a transação não existia e foi criada, precisa ser fechada para retornar ao pool */
  if (!this.connection) {
    cnx.close()
    cnx = null
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
  let values = ''
  let where = ''
  let vrg = ''
  let and = ''

  for (let col in row) {
    let val = row[col]

    values += vrg + '"' + col + '"' + ' = '
    values += (val.constructor.name === 'Number')
      ? val
      : (sdel + val + sdel)

    vrg = ', '
  }

  if (whereCondition) {
    for (let wkey in whereCondition) {
      let val = whereCondition[wkey]

      where += and + '"' + wkey + '"' + ' = '
      where += (val.constructor.name === 'Number')
        ? val
        : (sdel + val + sdel)

      and = ' AND '
    }
  }

  let sql = 'UPDATE "' + table + '" SET ' + values + ((whereCondition) ? ' WHERE ' + where : '')

  if (hasSqlInject(sql)) {
    return sqlInjectionError
  }

  let cnx = this.connection || getConnection(ds)
  let stmt = cnx.prepareStatement(sql)
  this.logFunction('update', 'executeUpdate', sql)
  let result = stmt.executeUpdate()

  /* se a transação não existia e foi criada, precisa ser fechada para retornar ao pool */
  if (!this.connection) {
    cnx.close()
    cnx = null
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
  let where = ''
  let and = ''
  let result

  if (whereCondition) {
    for (let wkey in whereCondition) {
      let val = whereCondition[wkey]

      where += and + '"' + wkey + '"' + ' = '
      where += (val.constructor.name === 'Number')
        ? val
        : (sdel + val + sdel)

      and = ' AND '
    }
  }

  let sql = 'DELETE FROM "' + table + ((whereCondition) ? '" WHERE ' + where : '"')

  if (hasSqlInject(sql)) {
    return sqlInjectionError
  }

  let cnx = this.connection || getConnection(ds)
  let stmt = cnx.prepareStatement(sql)

  this.logFunction('delete', 'executeUpdate', sql)
  result = stmt.executeUpdate()

  /* se a transação não existia e foi criada, precisa ser fechada para retornar ao pool */
  if (!this.connection) {
    cnx.close()
    cnx = null
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
    connection: cnx,
    stringDelimiter: this.stringDelimiter || "'",
    logFunction: this.logFunction
  }

  try {
    cnx.setAutoCommit(false)

    rs = {
      error: false,

      result: fncScript({
        'getInfoColumns': getInfoColumns.bind(ctx, ds),
        'insert': tableInsert.bind(ctx, ds),
        'select': sqlSelect.bind(ctx, ds),
        'update': tableUpdate.bind(ctx, ds),
        'delete': tableDelete.bind(ctx, ds),
        'execute': sqlExecute.bind(ctx, ds)
      }, context)
    }

    cnx.commit()
  } catch (ex) {
    rs = {
      error: true,
      execption: ex
    }

    cnx.rollback()
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
