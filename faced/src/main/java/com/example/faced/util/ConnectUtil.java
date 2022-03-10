package com.example.faced.util;


import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;

import java.io.InputStream;

public class ConnectUtil {
    private Connection conn;
    private String charset;
    private String ip;
    private String userName;
    private String password;

    public ConnectUtil(String ip, String userName, String password, String charset) {
        this.ip = ip;
        this.userName = userName;
        this.password = password;
        this.charset = charset;
    }

    public boolean login() throws Exception {
        conn = new Connection(ip);
        conn.connect();
        return conn.authenticateWithPassword(userName, password);
    }

    public String exec(String cmds) {
        InputStream in = null;
        String result = "";
        try {
            if (this.login()) {
                Session session = conn.openSession(); // 打开一个会话
                session.execCommand(cmds);
                System.out.println(cmds);
                in = session.getStdout();
                result = this.processStdout(in, charset);
                session.close();
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public String processStdout(InputStream in, String charset) {

        byte[] buf = new byte[1024];
        StringBuffer sb = new StringBuffer();
        try {
            while (in.read(buf) != -1) {
                sb.append(new String(buf, charset));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return sb.toString();
    }

    public boolean uploadFile(String localFilePath, String remoteFilePath) throws Exception {
        boolean bool = false;
        if (this.login()) {
            System.out.println(this.login());
            SCPClient scpClient = conn.createSCPClient();
            scpClient.put(localFilePath, remoteFilePath);
            bool = true;
        }
        return bool;
    }

    public boolean downloadFile(String remoteFilePath, String localFilePath) throws Exception {
        boolean bool = false;
        if (this.login()) {
            System.out.println(this.login());
            SCPClient scpClient = conn.createSCPClient();
            scpClient.get(remoteFilePath, localFilePath);
            bool = true;
        }
        return bool;
    }


}
