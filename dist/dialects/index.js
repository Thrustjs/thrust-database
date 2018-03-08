const DIALECTS = {
  mysql: true
}

exports = {
  getDialect: function(type) {
    return DIALECTS[type] ? require(type) : require('default')
  }
}
