package com.vandeilson.kafka.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping
@Slf4j
public class FirstController {

    @GetMapping
    public String getHelloWorld() {
        log.info("Entrando no controller e logando com o maior sucesso do mundo");
        return "Hello, world!";
    }

}
