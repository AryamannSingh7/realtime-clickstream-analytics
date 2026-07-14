package com.clickstream.generator.web;

import com.clickstream.generator.service.LoadGeneratorService;
import com.clickstream.generator.service.RampController;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Small operational API for inspecting and steering the load generator at runtime:
 * read current status, change the target rate, pause / resume emission, and run
 * scripted load profiles (ramp / burst) for benchmarks.
 */
@RestController
@RequestMapping("/api/generator")
public class GeneratorController {

    private final LoadGeneratorService generator;
    private final RampController ramp;

    public GeneratorController(LoadGeneratorService generator, RampController ramp) {
        this.generator = generator;
        this.ramp = ramp;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("running", generator.isRunning());
        body.put("paused", generator.isPaused());
        body.put("targetEps", generator.getTargetEps());
        body.put("virtualUsers", generator.getVirtualUsers());
        body.put("emitterThreads", generator.getEmitterThreads());
        body.put("activeProfile", ramp.activeProfile());
        body.put("topic", generator.getTopic());
        body.put("totalProduced", generator.getTotalProduced());
        body.put("sendErrors", generator.getSendErrors());
        return body;
    }

    @PostMapping("/rate")
    public Map<String, Object> setRate(@RequestParam("eps") int eps) {
        ramp.cancel();
        generator.setTargetEps(eps);
        return status();
    }

    /** Linearly ramp the rate from {@code from} to {@code to} over {@code durationSec} seconds. */
    @PostMapping("/ramp")
    public Map<String, Object> ramp(
            @RequestParam("from") int from,
            @RequestParam("to") int to,
            @RequestParam("durationSec") int durationSec) {
        ramp.ramp(from, to, durationSec);
        return status();
    }

    /** Spike to {@code eps} for {@code durationSec} seconds, then restore the previous rate. */
    @PostMapping("/burst")
    public Map<String, Object> burst(
            @RequestParam("eps") int eps,
            @RequestParam("durationSec") int durationSec) {
        ramp.burst(eps, durationSec);
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
