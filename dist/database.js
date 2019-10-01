/* jshint asi: true */

const Types = Java.type('java.sql.Types')
const Statement = Java.type('java.sql.Statement')
const DataSource = Java.type('org.apache.tomcat.jdbc.pool.DataSource')
const JTimestamp = Java.type('java.sql.Timestamp')

const localResource = {
  version: '0.3.2'
}

try {
  localResource.dsm = database_ds_cache || {}
} catch (e) {
  localResource.dsm = {}
  if (typeof dangerouslyLoadToGlobal === 'function') {
    dangerouslyLoadToGlobal('database_ds_cache', localResource.dsm)
  }
}

const dialects = {
  mysql: {
    name: 'mysql',
    scapeChar: '`',
    stringDelimiter: "'"
  },
  postgresql: {
    name: 'postgresql',
    scapeChar: '"',
    stringDelimiter: "'"
  },
  sqlite: {
    name: 'sqlite',
    scapeChar: '"',
    stringDelimiter: "'"
  },
  h2: {
    name: 'h2',
    scapeChar: '"',
    stringDelimiter: "'"
  }
}

function createDbInstance(options) {
  options.logFunction = options.logFunction || function(dbFunctionName, statementMethodName, sql) { }

  const ctx = {
    returnColumnLabel: options.returnColumnLabel || false,
    dialect: options.dialect ? dialects[options.dialect] : dialects.postgresql,
    logFunction: options.logFunction,
    dateAsString: options.hasOwnProperty('dateAsString') ? options.dateAsString : false
  }
  const ds = createDataSource(options)

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
  let cnx

  try {
    cnx = this.connection || getConnection(ds)

    const databaseMetaData = cnx.getMetaData()
    const infosCols = databaseMetaData.getColumns(null, null, table, null)
    const dialect = this.dialect.name

    const cols = []

    while (infosCols.next()) {
      const column = {}

      column.name = infosCols.getString('COLUMN_NAME')
      column.dataType = infosCols.getString('DATA_TYPE')
      column.size = infosCols.getString('COLUMN_SIZE')
      column.decimalDigits = infosCols.getString('DECIMAL_DIGITS')
      column.isNullable = infosCols.getString('IS_NULLABLE')
      column.isAutoIncrment = infosCols.getString('IS_AUTOINCREMENT')
      column.ordinalPosition = infosCols.getString('ORDINAL_POSITION')

      if (dialect !== 'postgresql') {
        column.isGeneratedColumn = infosCols.getString('IS_GENERATEDCOLUMN')
      }

      cols.push(column)
    }
    return cols
  } finally {
    /* se a transação não existia e foi criada, precisa ser fechada para retornar ao pool */
    if (!this.connection) {
      closeResource(cnx)
      cnx = null
    }
  }
}

function createDataSource(options) {
  const urlConnection = options.urlConnection

  /* coverage ignore if */
  if (localResource.dsm[urlConnection]) {
    return localResource.dsm[urlConnection]
  }

  options.logFunction('createDataSource', 'DataSource', urlConnection)

  const ds = new DataSource()
  const cfg = Object.assign({
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

  /* coverage ignore else */
  if (!cfg.decryptClassName) {
    ds.setPassword(cfg.password)
  } else {
    var DecryptClass = Java.type(cfg.decryptClassName)
    var descryptInstance = new DecryptClass()

    ds.setPassword(descryptInstance.decrypt(cfg.password))
  }

  localResource.dsm[urlConnection] = ds

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
  const connection = ds.getConnection()

  /* coverage ignore next */
  connection.setAutoCommit((autoCommit !== undefined) ? autoCommit : true)

  return connection
}

function setParameter(stmt, col, value, dialect) {
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
        stmt.setTimestamp(col, new JTimestamp(value.getTime()))
        break

      case 'Blob':
        /* coverage ignore next */
        stmt.setBinaryStream(col, value.fis, value.size)
        /* coverage ignore next */
        break

      default:
        if (dialect && dialect.name === 'postgresql' && value.constructor.name === 'Object') {
          const JPGObject = Java.type('org.postgresql.util.PGobject')
          var jsonObject = new JPGObject()

          jsonObject.setType('json')
          jsonObject.setValue(JSON.stringify(value))

          stmt.setObject(col, jsonObject)
        } else {
          /* coverage ignore next */
          stmt.setObject(col, value)
        }
        break
    }
  }
}

function bindParams(stmt, params, data, dialect) {
  let arrInc = 0

  const setValueArrValue = function(col) {
    return function(arrValue) {
      setParameter(stmt, col + arrInc, arrValue, dialect)
      arrInc++
    }
  }

  if (params && data && data.constructor.name === 'Object') {
    for (var index in params) {
      index = Number(index)

      var name = params[index]

      // FIX: não deveria considerar NULL ao invés de dar uma exception?
      // Nos casos existentes até o momento, o params e o data nunca terão informações diferentes
      /* coverage ignore if */
      if (!data.hasOwnProperty(name)) {
        throw new Error('Error while processing a query prameter. Parameter \'' + name + '\' don\'t exists on the parameters object')
      }

      const value = data[name]
      const col = index + 1

      if (value && value.constructor.name === 'Array') {
        value.forEach(setValueArrValue(col))
        arrInc = (arrInc > 0) ? --arrInc : arrInc
      } else {
        setParameter(stmt, col + arrInc, value, dialect)
      }
    }
  }

  return stmt
}

function prepareStatement(cnx, sql, data, returnGeneratedKeys, dialect) {
  let stmt
  const params = []

  if (data && data.constructor.name === 'Object') {
    const keys = Object.keys(data)
    const placeHolders = sql.match(/:\w+/g) || []

    placeHolders.forEach(function(namedParam) {
      const name = namedParam.slice(1)

      if (keys.indexOf(name) >= 0) {
        params.push(name)
      }
    })

    params.forEach(function(namedParam) {
      const val = data[namedParam]

      if (val && val.constructor.name === 'Array') {
        const questionArray = val.map(function() {
          return '?'
        })
        sql = sql.replace(':' + namedParam, questionArray.join(','))
      } else {
        sql = sql.replace(':' + namedParam, '?')
      }
    })
  }

  stmt = (returnGeneratedKeys) ? cnx.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS) : cnx.prepareStatement(sql)

  return bindParams(stmt, params, data, dialect)
}

function sqlInsert(ds, sql, data, returnGeneratedKeys) {
  let cnx, stmt, rsk, rows, affected

  try {
    cnx = this.connection || getConnection(ds)
    stmt = prepareStatement(cnx, sql, data, returnGeneratedKeys, this.dialect)
    this.logFunction('execute', 'executeUpdate', sql)
    affected = stmt.executeUpdate()

    rsk = stmt.getGeneratedKeys()
    rows = []
    if (rsk) {
      while (rsk.next()) {
        rows.push(rsk.getObject(1))
      }
    }
  } finally {
    closeResource(stmt)
    stmt = null

    if (!this.connection) {
      closeResource(cnx)
      cnx = null
    }
  }

  return {
    error: false,
    keys: rows,
    affectedRows: affected
  }
}

function sqlSelect(ds, sqlCmd, dataValues, extraData) {
  const schar = this.dialect.scapeChar
  let cnx, stmt, sql, data, rs, result
  const whereData = {}

  if (sqlCmd.match(/^SELECT|^\(SELECT|^WITH/i)) {
    sql = sqlCmd
    data = dataValues
  } else {
    const table = sqlCmd.split(' ')[0]
    const columns = dataValues || []
    let vrg = ''
    let cols = ''

    for (var i = 0; i < columns.length; i++) {
      cols += vrg + schar + columns[i].split(' ')[0] + schar
      vrg = ','
    }

    cols = (cols === '') ? '*' : cols

    let whereCondition = extraData || {}
    let where = ''
    let and = ''

    for (var wkey in whereCondition) {
      whereData['w_' + wkey] = whereCondition[wkey]
      where += and + schar + wkey + schar + ' = :w_' + wkey
      and = ' AND '
    }

    sql = 'SELECT ' + cols + ' FROM ' + schar + table + ((extraData) ? schar + ' WHERE ' + where : schar)
  }

  try {
    cnx = this.connection || getConnection(ds)
    stmt = prepareStatement(cnx, sql, Object.assign(whereData, data))

    this.logFunction('execute', 'executeQuery', sql)
    rs = stmt.executeQuery()

    result = fetchRows(rs, this.returnColumnLabel, this.dateAsString)
  } finally {
    closeResource(stmt)
    stmt = null

    if (!this.connection) {
      closeResource(cnx)
      cnx = null
    }
  }

  return result
}

function sqlExecute(ds, sql, data, returnGeneratedKeys) {
  let cnx, stmt, result
  const sqlSelectCtx = sqlSelect.bind(this, ds)
  const sqlInsertCtx = sqlInsert.bind(this, ds)

  if (sql) {
    sql = sql.trim()
  }

  if (sql.match(/^SELECT|^\(SELECT|^WITH/i)) {
    return sqlSelectCtx(sql, data)
  } else if (sql.substring(0, 6).toUpperCase() === 'INSERT') {
    return sqlInsertCtx(sql, data, returnGeneratedKeys)
  }

  try {
    cnx = this.connection || getConnection(ds)
    stmt = prepareStatement(cnx, sql.trim(), data)
    this.logFunction('execute', 'executeUpdate', sql)
    result = stmt.executeUpdate()
  } finally {
    closeResource(stmt)
    stmt = null

    if (!this.connection) {
      closeResource(cnx)
      cnx = null
    }
  }

  return {
    error: false,
    affectedRows: result
  }
}

function fetchRows(rs, returnColumnLabel, dateAsString) {
  var rsmd = rs.getMetaData()
  var numColumns = rsmd.getColumnCount()
  var columns = []
  var typesInfo = []
  var rows = []

  for (var cl = 1; cl < numColumns + 1; cl++) {
    if (returnColumnLabel) {
      columns[cl] = rsmd.getColumnLabel(cl)
    } else {
      columns[cl] = rsmd.getColumnName(cl)
    }

    typesInfo[cl] = {
      type: rsmd.getColumnType(cl),
      typeName: rsmd.getColumnTypeName(cl)
    }
  }

  while (rs.next()) {
    var row = {}

    for (var nc = 1; nc < numColumns + 1; nc++) {
      var value

      var name = columns[nc]
      var type = typesInfo[nc].type
      var typeName = typesInfo[nc].typeName

      if (type === Types.BINARY) {
        value = rs.getBytes(nc)
      } else {
        value = rs.getObject(nc)
      }

      if (rs.wasNull()) {
        row[name] = null
      } else if (value === true || value === false) {
        row[name] = value
      } else if ([Types.DATE, Types.TIME, Types.TIMESTAMP].indexOf(type) >= 0) { // Data
        /* coverage ignore if */
        if (dateAsString) {
          row[name] = value.toString()
        } else {
          row[name] = new Date(Number(value.getTime()))
        }
      } else if ([Types.LONGVARCHAR, Types.CHAR, Types.VARCHAR, Types.NVARCHAR, Types.LONGNVARCHAR].indexOf(type) >= 0) { // String/char...
        value = value.toString()

        /* Tipo JSON no MySQL */
        if (typeName === 'JSON') {
          try {
            value = JSON.parse(value)
          } catch (error) {
          }
        }

        row[name] = value
      } else if (type === Types.OTHER) { // json in PostgreSQL
        try {
          row[name] = JSON.parse(value)
        } catch (error) {
          row[name] = value
        }
      } else if (!isNaN(Number(value))) {
        row[name] = Number(value)
      } else {
        row[name] = value
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
function tableInsert(ds, table, itens, returnGeneratedKeys) {
  var logFunction = this.logFunction
  var dialect = this.dialect
  var schar = dialect.scapeChar
  // var sdel = this.dialect.stringDelimiter
  var cnx = this.connection || getConnection(ds)
  var affected = 0
  var stmt, sql

  var itIsDataArray = (itens.constructor.name === 'Array')

  function mountSql(table, params, data) {
    var placeHolders = Array.apply(null, new Array(params.length)).map(function() { return '?' })

    return ['INSERT INTO ', schar, table, schar,
      ' (', schar, params.join(schar + ', ' + schar), schar, ')',
      ' values (', placeHolders.join(', '), ')'
    ].join('')
  }

  function mountStmt(sql, params, data) {
    stmt = (returnGeneratedKeys) ? cnx.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS) : cnx.prepareStatement(sql)
    bindParams(stmt, params, data, dialect)
    return stmt
  }

  function getGeneratedKeys(stmt) {
    var keys = []

    if (returnGeneratedKeys) {
      var rsKeys = stmt.getGeneratedKeys()

      while (rsKeys.next()) {
        keys.push(rsKeys.getObject(1))
      }
    }

    return keys
  }

  var keys = []

  try {
    if (itIsDataArray) {
      itens.forEach(function(data) {
        var params = Object.keys(data)

        sql = mountSql(table, params, data)
        stmt = mountStmt(sql, params, data)
        logFunction('insert', 'executeUpdate', sql, data)
        affected += stmt.executeUpdate()
        keys = (returnGeneratedKeys) ? getGeneratedKeys(stmt) : keys
      })
    } else {
      var params = Object.keys(itens)

      sql = mountSql(table, params, itens)
      stmt = mountStmt(sql, params, itens)
      logFunction('insert', 'executeUpdate', sql, itens)
      affected += stmt.executeUpdate()
      keys = (returnGeneratedKeys) ? getGeneratedKeys(stmt) : keys
    }
  } finally {
    /* se a transação não existia e foi criada, precisa ser fechada para retornar ao pool */
    if (!this.connection) {
      closeResource(cnx)
      cnx = null
    }
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
  var schar = this.dialect.scapeChar
  // var sdel = this.dialect.stringDelimiter
  var whereData = {}
  var setData = {}
  var values = ''
  var where = ''
  var vrg = ''
  var and = ''

  for (var col in row) {
    setData['set_' + col] = row[col]
    values += vrg + schar + col + schar + ' = :set_' + col
    vrg = ', '
  }

  if (whereCondition) {
    for (var wkey in whereCondition) {
      whereData['w_' + wkey] = whereCondition[wkey]
      where += and + schar + wkey + schar + ' = :w_' + wkey
      and = ' AND '
    }
  }

  var sql = 'UPDATE ' + schar + table.split(' ')[0] + schar + ' SET ' + values + ((whereCondition) ? ' WHERE ' + where : '')

  var cnx, stmt, affected

  try {
    cnx = this.connection || getConnection(ds)
    stmt = prepareStatement(cnx, sql, Object.assign({}, setData, whereData))
    this.logFunction('update', 'executeUpdate', sql)
    affected = stmt.executeUpdate()
  } finally {
    closeResource(stmt)
    stmt = null

    /* se a transação não existia e foi criada, precisa ser fechada para retornar ao pool */
    if (!this.connection) {
      closeResource(cnx)
      cnx = null
    }
  }

  return {
    error: false,
    affectedRows: affected
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
  const schar = this.dialect.scapeChar
  // var sdel = this.dialect.stringDelimiter
  var whereData = {}
  var where = ''
  var and = ''

  if (whereCondition) {
    for (var wkey in whereCondition) {
      whereData['w_' + wkey] = whereCondition[wkey]
      where += and + schar + wkey + schar + ' = :w_' + wkey
      and = ' AND '
    }
  }

  var sql = 'DELETE FROM ' + schar + table.split(' ')[0] + ((whereCondition) ? schar + ' WHERE ' + where : schar)

  var cnx, stmt, affected

  try {
    cnx = this.connection || getConnection(ds)
    stmt = prepareStatement(cnx, sql, whereData)
    this.logFunction('delete', 'executeUpdate', sql)
    affected = stmt.executeUpdate()
  } finally {
    closeResource(stmt)
    stmt = null

    /* se a transação não existia e foi criada, precisa ser fechada para retornar ao pool */
    if (!this.connection) {
      closeResource(cnx)
      cnx = null
    }
  }

  return {
    error: false,
    affectedRows: affected
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
  const cnx = getConnection(ds)
  const ctx = {
    connection: cnx,
    dialect: this.dialect,
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
      exception: ex
    }

    cnx.rollback()
  } finally {
    closeResource(cnx)
  }

  return rs
}

function closeResource(resource) {
  if (resource != null) {
    resource.close()
  }
}

exports = {
  createDbInstance: createDbInstance,
  version: localResource.version
}
