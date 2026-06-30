package com.clickstream.generator.web;

import com.clickstream.generator.service.LoadGeneratorService;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Small operational API for inspecting and steering the load generator at runtime:
 * read current status, change the target rate, and pause / resume emission.
 */
@RestController
@RequestMapping("/api/generator")
public class GeneratorController {

    private final LoadGeneratorService generator;

    public GeneratorController(LoadGeneratorService generator) {
        this.generator = generator;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("running", generator.isRunning());
        body.put("paused", generator.isPaused());
        body.put("targetEps", generator.getTargetEps());
        body.put("virtualUsers", generator.getVirtualUsers());
        body.put("topic", generator.getTopic());
        body.put("totalProduced", generator.getTotalProduced());
        body.put("sendErrors", generator.getSendErrors());
        return body;
    }

    @PostMapping("/rate")
    public Map<String, Object> setRate(@RequestParam("eps") int eps) {
        generator.setTargetEps(eps);
        return status();
    }

    @PostMapping("/pause")
    public Map<String, Object> pause() {
        generator.pause();
        return status();
    }

    @PostMapping("/resume")
    public Map<String, Object> resume() {
        generator.resume();
        return status();
    }
}
