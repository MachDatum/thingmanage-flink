tEnv.executeSql("CREATE TABLE SinkTable(" +
      GenerateSchema(${tablename}.getSchema()) +
      ")" +
      "WITH(" +
      "'connector' = 'elasticsearch-7'," +
      "'hosts' = 'http://192.168.1.130:9200'," +
      "'index' = '${tablename}'" +
      ")");
${tablename}.executeInsert("SinkTable")