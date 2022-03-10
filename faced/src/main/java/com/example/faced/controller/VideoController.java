package com.example.faced.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

@Controller
public class VideoController {


    @RequestMapping(value = "/videos")
    public String videos(HttpServletRequest request, HttpServletResponse response) {
        try {
            String filePath = (String) request.getSession().getAttribute("filePath");
            FileInputStream fis = null;
            OutputStream os = null;
            fis = new FileInputStream(filePath);
            int size = fis.available(); // 得到文件大小
            byte data[] = new byte[size];
            fis.read(data); // 读数据
            fis.close();
            fis = null;
            response.setContentType("video/mp4"); // 设置返回的文件类型
            os = response.getOutputStream();
            os.write(data);
            os.flush();
            os.close();
            os = null;


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
