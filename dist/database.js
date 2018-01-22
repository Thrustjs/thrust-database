/**
 *
 * @author nery
 * @version 0.2.20180119
 *
 */

let Types = Java.type('java.sql.Types')
let Statement = Java.type('java.sql.Statement')
let DataSource = Java.type('org.apache.tomcat.jdbc.pool.DataSource')

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
  let testSqlInject = sql.match(/[\t\r\n]|(--[^\r\n]*)|(\/\*[\w\W]*?(?=\*)\*\/)/gi)

  return (testSqlInject != null)
}

function processNamedParameters(sql) {
  let literals = {};
  let id = 0;
  let key;

  sql = sql.replace(/(\'.*?\')/g, function (match) {
    key = '${' + (id++) + '}';
    literals[key] = match;
    return key;
  });

  let params = [];

  sql = sql.replace(/(:\w+)/g, function (match) {
    params.push(match.substring(1))
    return '?'
  })

  sql = sql.replace(/(\$\{\d+\})/g, function (match) {
    return literals[match];
  });

  return {
    sql: sql,
    params: params
  }
}

function prepareStatement(cnx, sql, data, returnGeneratedKeys) {
  let stmt
  let params;

  if (data) {
    let result = processNamedParameters(sql)

    sql = result.sql;
    params = result.params;
  }

  stmt = (returnGeneratedKeys)
    ? cnx.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
    : cnx.prepareStatement(sql)

  if (params && data && data.constructor.name === 'Object') {
    params.forEach(function (param, index) {
      if (!data.hasOwnProperty(param)) {
        throw new Error('Error while processing a query prameter. Parameter \'' + param + '\' don\'t exists on the parameters object')
      }

      let value = data[param]
      let col = index + 1

      bindParameterOnStatement(stmt, col, value)
    })
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

function bindParameterOnStatement (stmt, index, value) {
  if (value === undefined || value === null) {
    stmt.setObject(index, null)
  } else {
    switch (value.constructor.name) {
      case 'String':
        stmt.setString(index, value)
        break

      case 'Boolean':
        stmt.setBoolean(index, value)
        break

      case 'Number':
        if (Math.floor(value) === value) {
          stmt.setLong(index, value)
        } else {
          stmt.setDouble(index, value)
        }
        break

      case 'Date':
        stmt.setTimestamp(index, new java.sql.Timestamp(value.getTime()))
        break

      case 'Blob':
      stmt.setBinaryStream(index, value.fis, value.size)
        break

      default:
        stmt.setObject(index, value)
        break
    }
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
  let cnx = this.connection || getConnection(ds)
  let keys = []
  let stmt
  let affected
  let sql

  function buildSqlCommand (reg) {
    let vrg = ''
    let cols = ''
    let values = ''
    let value

    for (let key in reg) {
      value = reg[key]
      cols += vrg + '"' + key + '"'
      values += (vrg + '?')

      vrg = ','
    }

    // print( "INSERT INTO " + table + " (" + cols + ") " + "VALUES (" + values + ") " )
    return 'INSERT INTO "' + table + '" (' + cols + ') ' + 'VALUES (' + values + ')'
  }

  if (itens.constructor.name === 'Array') {
    let values = []

    itens.forEach(function (tupla, idx) {
      if (idx === 0) {
        sql = buildSqlCommand(tupla)
      }

      let props = []

      for (let key in tupla) {
        props.push(tupla[key])
      }

      values.push(props)
    })

    stmt = cnx.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)

    values.forEach(function (tuple) {
      tuple.forEach(function (value, colIndex) {
        bindParameterOnStatement(stmt, colIndex + 1, value)
      })

      logFunction('insert', 'addBatch', sql)
      stmt.addBatch()
    })

    logFunction('insert', 'executeBatch', '')
    affected = stmt.executeBatch()
  } else {
    let item = itens

    sql = buildSqlCommand(item)

    stmt = cnx.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)

    let colIndex = 1
    for (let key in item) {
      bindParameterOnStatement(stmt, colIndex++, item[key])
    }

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
  let values = ''
  let where = ''
  let vrg = ''
  let and = ''

  for (let col in row) {
    values += vrg + '"' + col + '"' + ' = ?'
    vrg = ', '
  }

  if (whereCondition) {
    for (let wkey in whereCondition) {
      where += and + '"' + wkey + '"' + ' = ?'
      and = ' AND '
    }
  }

  let sql = 'UPDATE "' + table + '" SET ' + values + ((whereCondition) ? ' WHERE ' + where : '')

  if (whereCondition && hasSqlInject(where)) {
    return sqlInjectionError
  }

  let cnx = this.connection || getConnection(ds)
  let stmt = cnx.prepareStatement(sql)

  let colIndex = 1
  for (let key in row) {
    bindParameterOnStatement(stmt, colIndex++, row[key])
  }

  if (whereCondition) {
    for (let wkey in whereCondition) {
      bindParameterOnStatement(stmt, colIndex++, whereCondition[wkey])
    }
  }

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
  let where = ''
  let and = ''
  let result

  if (whereCondition) {
    for (let wkey in whereCondition) {
      where += and + '"' + wkey + '"' + ' = ?'
      and = ' AND '
    }
  }

  let sql = 'DELETE FROM "' + table + ((whereCondition) ? '" WHERE ' + where : '"')

  if (hasSqlInject(sql)) {
    return sqlInjectionError
  }

  let cnx = this.connection || getConnection(ds)
  let stmt = cnx.prepareStatement(sql)

  if (whereCondition) {
    let colIndex = 1

    for (let wkey in whereCondition) {
      bindParameterOnStatement(stmt, colIndex++, whereCondition[wkey])
    }
  }

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
