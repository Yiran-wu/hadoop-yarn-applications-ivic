package org.apache.hadoop.yarn.applications.ivic;

import java.io.ByteArrayOutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class Test extends Thread{
    private static final int MAX_PRIMES = 1000000;
    private static final int TEN_SECONDS = 10000;
    
    public volatile boolean finished = false;
    
    public void run() {
        int[] primes = new int[MAX_PRIMES];
        int count = 0;
        
        for (int i = 2; count < MAX_PRIMES; i++) {
            // Check to see if the timer has expired
            if (finished) {
                break;
            }
            
            boolean prime = true;
            for (int j = 0; j < count; j++) {
                if (i % primes[j] == 0) {
                    prime = false;
                    break;
                }
            }
            
            if (prime) {
                primes[count++] = i;
                System.out.println("Found prime: " + i);
            }
        }
        
    }

    
    public static int compareVersion(String version1, String version2) {
        if(strToInt(version1)[0] > strToInt(version2)[0]) return 1;
        else if(strToInt(version1)[0] < strToInt(version2)[0]) return -1;
        else {
            if(strToInt(version1)[1] > strToInt(version2)[1]) return 1;
            else if(strToInt(version1)[1] < strToInt(version2)[1]) return -1;
            else return 0;
        }
    }
    
    public static int[] strToInt(String version) {
        int[] num = {0, 0};
        int i = 0;
        while((i < version.length()) && (version.charAt(i) != '.')) {
            num[0] = num[0] * 10 + version.charAt(i) - '0';
            i++;
        }
        i++;
        while(i < version.length()) {
            num[1] = num[1] * 10 + version.charAt(i) - '0';
            i++;
        }
        System.out.println("num:" + num[0] + " " + num[1]);
        return num;
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        /*
        Test test = new Test();
        test.start();
        try {
            Thread.sleep(TEN_SECONDS);
        }
        catch (InterruptedException e) {
            // fall through
        }
        test.finished = true;
        */
        
        //System.out.println("*****" + compareVersion("1.0", "1.10"));
        
        ConnectDataBase con = new ConnectDataBase();
        String sql = "select id, target_object_id, target_object_type, operation, options from jobs where status = 'pending' and user_id = 1";
        ResultSet rs = con.executeQuery(sql);
        try {
            while (rs.next()) {
                System.out.print("执行结果：" + rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
                //String vmSetting = generateVmSetting(con, rs);
                //System.out.println(vmSetting);
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    private static String generateVmSetting(ConnectDataBase con, ResultSet rs) {
        if (con == null) {
            con = new ConnectDataBase();
        }
        String xmlStr = null;
        String sql;
        ResultSet rSet;
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = null;
        try {
            builder = dbf.newDocumentBuilder();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Document doc = builder.newDocument();

        Element root = doc.createElement("vNode");
        doc.appendChild(root); // 将根元素添加到文档上

        try {       
            Element hostName = doc.createElement("Hostname");
            hostName.setTextContent(rs.getString("hostname"));
            root.appendChild(hostName);

            Element password = doc.createElement("Password");
            password.setTextContent(rs.getString("vnc_password"));
            root.appendChild(password);

            Element mem = doc.createElement("Mem");
            mem.setTextContent(rs.getString("mem_total"));
            root.appendChild(mem);

            Element vcpu = doc.createElement("vCPU");
            vcpu.setTextContent(rs.getString("vcpu"));
            root.appendChild(vcpu);
            // ******************************************
            // 以下都是vm_temps表中的信息
            // ******************************************
            if (rs.getString("vm_temp_id") != null) {
                sql = "select * from vm_temps where id = " + rs.getString("vm_temp_id");
                System.out.println(sql);
                ResultSet set = con.executeQuery(sql);
                while (set.next()) {
                    Element vTemplateRef = doc.createElement("vTemplateRef");
                    vTemplateRef.setTextContent(set.getString("url"));
                    root.appendChild(vTemplateRef);
                        
                    Element os = doc.createElement("OS");
                    root.appendChild(os);
                        
                    Element type = doc.createElement("Type");
                    type.setTextContent(set.getString("os_type"));
                    os.appendChild(type);
                        
                    Element distribution = doc.createElement("Distribution");
                    distribution.setTextContent(set.getString("os_distribution"));
                    os.appendChild(distribution);
                        
                    Element release = doc.createElement("Release");
                    release.setTextContent(set.getString("os_release"));
                    os.appendChild(release);
                        
                    Element kernel = doc.createElement("Kernel");
                    kernel.setTextContent(set.getString("os_kernel"));
                    os.appendChild(kernel);
                        
                    Element packages = doc.createElement("Packages");
                    packages.setTextContent(set.getString("os_packages"));
                    os.appendChild(packages);
                }
            }
            else {
                Element devType = doc.createElement("DevType");
                devType.setTextContent(rs.getString("disk_dev_type"));
                root.appendChild(devType);
            }
                
            // **************************************************
            // 根据vm_id读取vdisks表的信息
            // TODO 原来需要知道把虚拟机创建的地点，现在应该不需要
            // 这里将volume和cdrom类型的vdisk同时取出，并按type和position排序，
            // **************************************************
            int index = 0;
            sql = "select * from vdisks where virtual_machine_id = " + rs.getString("id") + " order by img_type, position";
            rSet = con.executeQuery(sql);
            while (rSet.next()) {
                Element vdisk = doc.createElement("vDisk");
                vdisk.setAttribute("id", "\'" + index + "\'");
                root.appendChild(vdisk);
                
                Element uuid = doc.createElement("UUID");
                uuid.setTextContent(rSet.getString("uuid"));
                vdisk.appendChild(uuid);
                    
                Element type = doc.createElement("Type");
                if (rSet.getString("vdisk_type").equals("volumn"))
                    type.setTextContent(rSet.getString("img_type"));
                else
                    type.setTextContent("cdrom");
                vdisk.appendChild(type);
                    
                // TODO 路径这里先写死！
                Element path = doc.createElement("Path");
                if (rSet.getString("vdisk_type").equals("volumn"))
                    path.setTextContent("/var/lib/ivic/vstore/" + rSet.getString("uuid") + ".img");
                else
                    path.setTextContent("/var/lib/ivic/vstore/" + rSet.getString("uuid") + ".iso");
                vdisk.appendChild(path);
                    
                if (rs.getString("vm_temp_id") != null && rSet.getString("base_id") != null) {
                    Element basePath = doc.createElement("BasePath");
                    // TODO 在vdisk表中存在base_id，但是需要知道base的uuid，所以，要么重新查询数据库，要么在vdisk表中增加一个字段；先按前者查询
                    sql = "select * from vdisks where id = " + rSet.getString("base_id");
                    ResultSet set = con.executeQuery(sql);
                    while (set.next()) {
                        if (rSet.getString("vdisk_type").equals("volumn"))
                            basePath.setTextContent("/var/lib/ivic/vstore/" + set.getString("uuid") + ".img");
                        else
                            basePath.setTextContent("/var/lib/ivic/vstore/" + set.getString("uuid") + ".iso");
                    }
                    vdisk.appendChild(basePath);
                }
                    
                if (rSet.getString("img_type").equals("raw") || rSet.getString("img_type").equals("rootfs")) {
                    Element size = doc.createElement("Size");
                    size.setTextContent(rSet.getString("size"));
                    vdisk.appendChild(size);
                }
                index++;
            }

            sql = "select vnics.*, vnets.vswitch_id from vnics, vnets where vnics.virtual_machine_id = " + rs.getString("id") + " and vnics.vnet_id = vnets.id";
            rSet = con.executeQuery(sql);
            while (rSet.next()) {
                Element nic = doc.createElement("NIC");
                nic.setAttribute("id", "\'" + (rSet.getRow() - 1) + "\'");
                root.appendChild(nic);
                    
                Element mac = doc.createElement("MAC");
                mac.setTextContent(rSet.getString("mac_address"));
                nic.appendChild(mac);
                
                Element addr = doc.createElement("Address");
                addr.setTextContent(rSet.getString("ip_address"));
                nic.appendChild(addr);
                
                Element netmask = doc.createElement("Netmask");
                netmask.setTextContent(rSet.getString("netmask"));
                nic.appendChild(netmask);
                
                Element gateway = doc.createElement("GateWay");
                gateway.setTextContent(rSet.getString("gateway"));
                nic.appendChild(gateway);
                
                Element dns = doc.createElement("DNS");
                dns.setTextContent("8.8.8.8");
                nic.appendChild(dns);
                
                Element vswitch = doc.createElement("vSwitchRef");
                if (rSet.getString("vswitch_id") != null) {
                    sql = "select uuid from vswitches where id = " + rSet.getString("vswitch_id");
                    ResultSet set = con.executeQuery(sql);
                    while (set.next()) {
                        vswitch.setTextContent(set.getString(1));
                    }
                }
                nic.appendChild(vswitch);
                
                Element type = doc.createElement("vnetType");
                type.setTextContent(rSet.getString("gateway"));
                nic.appendChild(type);
            }
        } catch (DOMException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        TransformerFactory tf = TransformerFactory.newInstance();      
        Transformer t;
        try {
            t = tf.newTransformer();
            t.setOutputProperty("encoding", "UTF-8");//解决中文问题，试过用GBK不行
            t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            ByteArrayOutputStream  bos  =  new  ByteArrayOutputStream();     
            t.transform(new DOMSource(doc), new StreamResult(bos));
            xmlStr = bos.toString();
            //xmlStr = xmlStr.replaceAll("^<\\?.*", "");
            return xmlStr;
        } catch (Exception e) {
            e.printStackTrace();
        }
          
        return xmlStr;
    }
}

