// TODO: Realizar o fetch de forma correta, de acordo com os java.sql.Types
// numericTypes
//  -5, 3, 8, 6, 4, 2, 7, 5, -6

// dateTypes
//     91, 92, 93

// stringTypes
//     -1, 1, 12, -9, -16

// blobTypes
//     2004, -2, -4, -3

// clobTypes
//     2005

var Types = Java.type('java.sql.Types')
var Statement = Java.type('java.sql.Statement')
var DataSource = Java.type('org.apache.tomcat.jdbc.pool.DataSource')

var config = getConfig()

config.dsm = config.dsm || {}

var dialects = {
  mysql: {
    scapeChar: '`',
    stringDelimiter: "'"
  },
  postgresql: {
    scapeChar: '"',
    stringDelimiter: "'"
  },
  sqlite: {
    scapeChar: '"',
    stringDelimiter: "'"
  },
  h2: {
    scapeChar: '"',
    stringDelimiter: "'"
  }
}

function createDbInstance(options) {
  options.logFunction = options.logFunction || function(dbFunctionName, statementMethodName, sql) { }

  var ctx = {
    returnColumnLabel: options.returnColumnLabel || false,
    dialect: options.dialect ? dialects[options.dialect] : dialects.postgresql,
    logFunction: options.logFunction
  }
  var ds = createDataSource(options)

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
  var cnx = getConnection(ds)
  var databaseMetaData = cnx.getMetaData()
  var infosCols = databaseMetaData.getColumns(null, null, table, null)
  var cols = []

  while (infosCols.next()) {
    var column = {}

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
  var urlConnection = options.urlConnection

  if (config.dsm[urlConnection]) {
    return config.dsm[urlConnection]
  }

  options.logFunction('createDataSource', 'DataSource', urlConnection)

  var ds = new DataSource()
  var cfg = Object.assign({
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
    var DecryptClass = Java.type(cfg.decryptClassName)
    var descryptInstance = new DecryptClass()

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
  var connection = ds.getConnection()

  connection.setAutoCommit((autoCommit !== undefined) ? autoCommit : true)

  return connection
}

function setParameter(stmt, col, value) {
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

function bindParams(stmt, params, data) {
  var arrInc = 0
  if (params && data && data.constructor.name === 'Object') {
    for (var index in params) {
      index = Number(index)

      var name = params[index]

      // FIX: não deveria considerar NULL ao invés de dar uma exception??
      if (!data.hasOwnProperty(name)) {
        throw new Error('Error while processing a query prameter. Parameter \'' + name + '\' don\'t exists on the parameters object')
      }

      var value = data[name]
      var col = index + 1

      if (value && value.constructor.name === 'Array') {
        value.forEach(function(arrValue) {
          setParameter(stmt, col + arrInc, arrValue)
          arrInc++
        })
        arrInc = (arrInc > 0) ? --arrInc : arrInc
      } else {
        setParameter(stmt, col + arrInc, value)
      }
    }
  }

  return stmt
}

function prepareStatement(cnx, sql, data, returnGeneratedKeys) {
  var stmt
  var params = []

  if (data && data.constructor.name === 'Object') {
    var keys = Object.keys(data)
    var placeHolders = sql.match(/:\w+/g) || []

    placeHolders.forEach(function(namedParam) {
      var name = namedParam.slice(1)

      if (keys.indexOf(name) >= 0) {
        params.push(name)
      }
    })

    params.forEach(function(namedParam) {
      var val = data[namedParam]

      if (val && val.constructor.name === 'Array') {
        var questionArray = val.map(function() {
          return '?'
        })
        sql = sql.replace(':' + namedParam, questionArray.join(','))
      } else {
        sql = sql.replace(':' + namedParam, '?')
      }
    })
  }

  stmt = (returnGeneratedKeys)
    ? cnx.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
    : cnx.prepareStatement(sql)

  return bindParams(stmt, params, data)
}

function sqlInsert(ds, sql, data, returnGeneratedKeys) {
  var cnx, stmt, rsk, rows, affected

  try {
    cnx = this.connection || getConnection(ds)
    stmt = prepareStatement(cnx, sql, data, returnGeneratedKeys)
    this.logFunction('execute', 'executeUpdate', sql)
    affected = stmt.executeUpdate()

    rsk = stmt.getGeneratedKeys()
    rows = []
  
    while (rsk && rsk.next()) {
      rows.push(rsk.getObject(1))
    }
  } finally {
    closeResource(stmt);
    stmt = null
  
    if (!this.connection) {
      closeResource(cnx);
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
  var schar = this.dialect.scapeChar
  var cnx, stmt, sql, data, rs, result
  var whereData = {}

  if (sqlCmd.match(/^SELECT|^\(SELECT|^WITH/i)) {
    sql = sqlCmd
    data = dataValues
  } else {
    var table = sqlCmd.split(' ')[0]
    var columns = dataValues || []
    var vrg = ''
    var cols = ''

    for (var i = 0; i < columns.length; i++) {
      cols += vrg + schar + columns[i].split(' ')[0] + schar
      vrg = ','
    }

    cols = (cols === '') ? '*' : cols

    var whereCondition = extraData || {}
    var where = ''
    var and = ''

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

    result = fetchRows(rs, this.returnColumnLabel)
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
  var cnx, stmt, result
  var sqlSelectCtx = sqlSelect.bind(this, ds)
  var sqlInsertCtx = sqlInsert.bind(this, ds)

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

function fetchRows(rs, returnColumnLabel) {
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

      var type = typesInfo[nc].type;
      var typeName = typesInfo[nc].typeName;

      if (type === Types.BINARY) {
        value = rs.getBytes(nc)
      } else {
        value = rs.getObject(nc)
      }

      if (rs.wasNull()) {
        row[columns[nc]] = null
      } else if ([Types.DATE, Types.TIME, Types.TIMESTAMP].indexOf(type) >= 0) { //Data
        row[columns[nc]] = value.toString()
      } else if ([Types.LONGVARCHAR, Types.CHAR, Types.VARCHAR, Types.NVARCHAR, Types.LONGNVARCHAR].indexOf(type) >= 0) { //String/char...
        value = value.toString()

        if (typeName == 'JSON' ) {
          try {
            value = JSON.parse(value)
          } catch (error) {
          }
        }

        row[columns[nc]] = value
      } else if (type === Types.OTHER) { // json in PostgreSQL
        try {
          row[columns[nc]] = JSON.parse(value)
        } catch (error) {
          row[columns[nc]] = value
        }
      } else if (!isNaN(value)) {
        row[columns[nc]] = Number(value)
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
function tableInsert(ds, table, itens, returnGeneratedKeys) {
  var logFunction = this.logFunction
  var schar = this.dialect.scapeChar
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
    stmt = (returnGeneratedKeys)
      ? cnx.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
      : cnx.prepareStatement(sql)

    bindParams(stmt, params, data)

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

  var cnx, stmt, affected;

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
  var schar = this.dialect.scapeChar
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

  var cnx, stmt, affected;

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
  var rs
  var cnx = getConnection(ds)
  var ctx = {
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
    cnx = null
  }

  return rs
}

function closeResource(resource) {
  if (resource != null) {
    resource.close();
  }
}

/**
 * @param {FileInputStream} fis
 * @param {int} size
 */
function Blob(fis, size) {
  this.fis = fis
  this.size = size
}

function hookErrorFunc(error) {
  var st = error.getStackTrace()
  var regex = /.*jdk\.nashorn\.internal\.scripts\.Script\$Recompilation.*\^eval?\\_\.(\w+)\(<eval>.*<eval>?:(\d+)\)$/g;
  var groups
  var errorDB

  for (var i = 0; i < st.length; i++) {
    var trace = st[i]

    if ((groups = regex.exec(trace)) !== undefined) {
      break
    }
  }

  errorDB = new Error([
    '[thrust] ', scriptInfo.scriptFile, '_.', groups[1], ' #' + groups[2], '\n' + error.toString()].join(''),
    scriptInfo.scriptFile + ' =>  _.' + groups[1],
    groups[2]
  )
  errorDB.stackTrace = Java.from(st)

  return errorDB
}

var hookFunction = function(options) {
  var target = createDbInstance(options)
  var hook = {}

  Object.getOwnPropertyNames(target).forEach(function(prop) {
    // print('PROP =>', prop)

    if (target[prop].constructor.name === 'Function') {
      hook[prop] = function() {
        try {
          return target[prop].apply(null, arguments)
        } catch (error) {
          throw hookErrorFunc(error)
        }
      }
    } else {
      hook[prop.name] = prop
    }
  })

  return hook
}

exports = {
  // createDbInstance: hookFunction
  createDbInstance: createDbInstance
}
