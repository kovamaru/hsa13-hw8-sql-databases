package com.hw8.app;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class UserService {

  private final JdbcTemplate jdbcTemplate;

  public String measureQueryTime(String query) {
    Instant start = Instant.now();
    int count = jdbcTemplate.queryForList(query).size();
    Instant end = Instant.now();
    Duration duration = Duration.between(start, end);
    return String.format("Query executed in: %d ms, rows: %d", duration.toMillis(), count);
  }

  public void manageIndex(String type, boolean create) {
    String indexName = "idx_dob_" + type.toLowerCase();
    String sql;
    if (create) {
      sql = String.format("CREATE INDEX %s ON users(date_of_birth) USING %s;", indexName, type.toUpperCase());
    } else {
      sql = String.format("DROP INDEX %s ON users;", indexName);
    }
    jdbcTemplate.execute(sql);
  }

  public void insertRandomUser() {
    String name = "User_" + UUID.randomUUID().toString().substring(0, 8);
    String email = UUID.randomUUID().toString().substring(0, 8) + "@example.com";
    String dob = "1990-01-01";
    jdbcTemplate.update(
        "INSERT INTO users (name, email, date_of_birth) VALUES (?, ?, ?)",
        name, email, dob
    );
  }

  public void setFlushLog(int value) {
    jdbcTemplate.execute(String.format("SET GLOBAL innodb_flush_log_at_trx_commit = %d;", value));
  }
}
