package com.example.faced.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class IndexController {

    @RequestMapping("/")
    public String index(){
        return "index";
    }

    @RequestMapping("/imagesIndex")
    public String imagesIndex(){
        return "imagesIndex";
    }

    @RequestMapping("/videosIndex")
    public String videosIndex(){
        return "videosIndex";
    }



}
