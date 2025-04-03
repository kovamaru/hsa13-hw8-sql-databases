package com.hw8.app;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class UserController {

  private final UserService userService;

  @GetMapping("/select/no-index")
  public String selectWithoutIndex() {
    return userService.measureQueryTime("SELECT * FROM users WHERE date_of_birth BETWEEN '1990-01-01' AND '1991-01-01' LIMIT 1000");
  }

  @GetMapping("/select/btree-index")
  public String selectWithBtreeIndex() {
    return userService.measureQueryTime("SELECT * FROM users FORCE INDEX(idx_dob_btree) WHERE date_of_birth BETWEEN '1990-01-01' AND '1991-01-01' LIMIT 1000");
  }

  @GetMapping("/select/hash-index")
  public String selectWithHashIndex() {
    return userService.measureQueryTime("SELECT * FROM users FORCE INDEX(idx_dob_hash) WHERE date_of_birth BETWEEN '1990-01-01' AND '1991-01-01' LIMIT 1000");
  }

  @PostMapping("/index/create")
  public void createIndex(@RequestParam String type) {
    userService.manageIndex(type, true);
  }

  @PostMapping("/index/drop")
  public void dropIndex(@RequestParam String type) {
    userService.manageIndex(type, false);
  }

  @PostMapping("/insert")
  public void insertOneUser() {
    userService.insertRandomUser();
  }

  @PostMapping("/config/flush-log")
  public void setFlushLog(@RequestParam int value) {
    userService.setFlushLog(value);
  }
}
