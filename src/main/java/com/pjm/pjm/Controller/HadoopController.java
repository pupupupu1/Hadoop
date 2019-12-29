package com.pjm.pjm.Controller;


import com.pjm.pjm.Pojo.BaseReturnVO;
import com.pjm.pjm.Util.HadoopUtil;
import com.pjm.pjm.Util.Result;
import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.fs.*;

import org.apache.hadoop.io.IOUtils;
import org.springframework.web.bind.annotation.PostMapping;

import org.springframework.web.bind.annotation.RequestMapping;

import org.springframework.web.bind.annotation.RequestParam;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 类或方法的功能描述 :TODO
 *
 * @date: 2018-11-28 13:51
 */

@RestController

@RequestMapping("/hadoop")

public class HadoopController {
    /**
     * 创建文件夹
     *
     * @param path
     * @return
     * @throws Exception
     */

    @RequestMapping("/mkdir")

    public Result mkdir(String path) throws Exception {
        System.out.println("path:" + path);

        if (StringUtils.isEmpty(path)) {

            Result result = new Result();

            result.setCode("500");

            result.setMessage("请求参数为空");

            result.setData("请求参数为空");

            result.setSuccess(false);

            return result;

        }

        // 文件对象

        FileSystem fs = HadoopUtil.getFileSystem();

        // 目标路径

        Path newPath = new Path(path);

        // 创建空文件夹

        boolean isOk = fs.mkdirs(newPath);

        fs.close();

        if (isOk) {

            Result result = new Result();

            result.setMessage("create dir success");

            result.setCode("200");

            result.setSuccess(true);

            return result;

        } else {

            Result result = new Result();

            result.setMessage("create dir fail");

            result.setCode("500");

            result.setSuccess(false);

            return result;

        }

    }

    /**
     * 创建文件
     *
     * @param path
     * @return
     * @throws Exception
     */

    @PostMapping("/createFile")

    public Result createFile(@RequestParam("path") String path, @RequestParam("file") MultipartFile file) throws Exception {

        if (StringUtils.isEmpty(path) || null == file.getBytes()) {

            Result result = new Result();

            result.setMessage("请求参数为空");

            result.setCode("500");

            result.setSuccess(false);

            return result;

        }

        String fileName = file.getOriginalFilename();

        FileSystem fs = HadoopUtil.getFileSystem();

        // 上传时默认当前目录，后面自动拼接文件的目录

        Path newPath = new Path(path + "/" + fileName);

        // 打开一个输出流

        FSDataOutputStream outputStream = fs.create(newPath);

        outputStream.write(file.getBytes());

        outputStream.close();

        fs.close();

        Result result = new Result();

        result.setMessage("create file success");

        result.setCode("200");

        result.setSuccess(true);

        return result;

    }

    /**
     * 读取HDFS文件内容
     *
     * @param path
     * @return
     * @throws Exception
     */

    @PostMapping("/readFile")

    public Result readFile(@RequestParam("path") String path) throws Exception {

        FileSystem fs = HadoopUtil.getFileSystem();

        Path newPath = new Path(path);

        InputStream in = null;

        try {

            in = fs.open(newPath);

            IOUtils.copyBytes(in, System.out, 4096);


        } finally {

            IOUtils.closeStream(in);

            fs.close();

        }


        Result result = new Result();

        result.setMessage("读取成功");

        result.setCode("200");

        result.setSuccess(true);

        result.setData(System.out.toString());

        return result;

    }

    /**
     * 读取HDFS目录信息
     *
     * @param path
     * @return
     * @throws Exception
     */

    @PostMapping("/readPathInfo")

    public BaseReturnVO readPathInfo(@RequestParam("path") String path) throws Exception {

        FileSystem fs = HadoopUtil.getFileSystem();

        Path newPath = new Path(path);

        FileStatus[] statusList = fs.listStatus(newPath);

        List<Map<String, Object>> list = new ArrayList<>();

        if (null != statusList && statusList.length > 0) {

            for (FileStatus fileStatus : statusList) {

                Map<String, Object> map = new HashMap<>();

                map.put("filePath", fileStatus.getPath());

                map.put("fileStatus", fileStatus.toString());

                list.add(map);

            }

            return new BaseReturnVO(list);

        } else {

            return new BaseReturnVO("目录内容为空");

        }

    }

    /**
     * 读取文件列表
     *
     * @param path
     * @return
     * @throws Exception
     */

    @PostMapping("/listFile")

    public BaseReturnVO listFile(@RequestParam("path") String path) throws Exception {

        if (StringUtils.isEmpty(path)) {

            return new BaseReturnVO("请求参数为空");

        }

        FileSystem fs = HadoopUtil.getFileSystem();

        Path newPath = new Path(path);

        // 递归找到所有文件

        RemoteIterator<LocatedFileStatus> filesList = fs.listFiles(newPath, true);

        List<Map<String, String>> returnList = new ArrayList<>();

        while (filesList.hasNext()) {

            LocatedFileStatus next = filesList.next();

            String fileName = next.getPath().getName();

            Path filePath = next.getPath();

            Map<String, String> map = new HashMap<>();

            map.put("fileName", fileName);

            map.put("filePath", filePath.toString());

            returnList.add(map);

        }

        fs.close();

        return new BaseReturnVO(returnList);

    }

    /**
     * 重命名文件
     *
     * @param oldName
     * @param newName
     * @return
     * @throws Exception
     */

    @PostMapping("/renameFile")

    public BaseReturnVO renameFile(@RequestParam("oldName") String oldName, @RequestParam("newName") String newName) throws Exception {

        if (StringUtils.isEmpty(oldName) || StringUtils.isEmpty(newName)) {

            return new BaseReturnVO("请求参数为空");

        }

        FileSystem fs = HadoopUtil.getFileSystem();

        Path oldPath = new Path(oldName);

        Path newPath = new Path(newName);

        boolean isOk = fs.rename(oldPath, newPath);

        fs.close();

        if (isOk) {

            return new BaseReturnVO("rename file success");

        } else {

            return new BaseReturnVO("rename file fail");

        }

    }

    /**
     * 删除文件
     *
     * @param path
     * @return
     * @throws Exception
     */

    @PostMapping("/deleteFile")

    public BaseReturnVO deleteFile(@RequestParam("path") String path) throws Exception {

        FileSystem fs = HadoopUtil.getFileSystem();

        Path newPath = new Path(path);

        boolean isOk = fs.deleteOnExit(newPath);

        fs.close();

        if (isOk) {

            return new BaseReturnVO("delete file success");

        } else {

            return new BaseReturnVO("delete file fail");

        }

    }

    /**
     * 上传文件
     *
     * @param path
     * @param uploadPath
     * @return
     * @throws Exception
     */

    @PostMapping("/uploadFile")

    public BaseReturnVO uploadFile(@RequestParam("path") String path, @RequestParam("uploadPath") String uploadPath) throws Exception {

        FileSystem fs = HadoopUtil.getFileSystem();

        // 上传路径

        Path clientPath = new Path(path);

        // 目标路径

        Path serverPath = new Path(uploadPath);


        // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false

        fs.copyFromLocalFile(false, clientPath, serverPath);

        fs.close();

        return new BaseReturnVO("upload file success");

    }

    /**
     * 下载文件
     *
     * @param path
     * @param downloadPath
     * @return
     * @throws Exception
     */

    @RequestMapping("/downloadFile")

    public BaseReturnVO downloadFile(@RequestParam("path") String path, @RequestParam("downloadPath") String downloadPath) throws Exception {

        FileSystem fs = HadoopUtil.getFileSystem();

        // 上传路径

        Path clientPath = new Path(path);

        // 目标路径

        Path serverPath = new Path(downloadPath);


        // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false

        fs.copyToLocalFile(false, clientPath, serverPath);

        fs.close();

        return new BaseReturnVO("download file success");

    }

    /**
     * HDFS文件复制
     *
     * @param sourcePath
     * @param targetPath
     * @return
     * @throws Exception
     */

    @PostMapping("/copyFile")

    public BaseReturnVO copyFile(@RequestParam("sourcePath") String sourcePath, @RequestParam("targetPath") String targetPath) throws Exception {

        FileSystem fs = HadoopUtil.getFileSystem();

        // 原始文件路径

        Path oldPath = new Path(sourcePath);

        // 目标路径

        Path newPath = new Path(targetPath);


        FSDataInputStream inputStream = null;

        FSDataOutputStream outputStream = null;

        try {

            inputStream = fs.open(oldPath);

            outputStream = fs.create(newPath);


            IOUtils.copyBytes(inputStream, outputStream, 1024 * 1024 * 64, false);

            return new BaseReturnVO("copy file success");

        } finally {

            inputStream.close();

            outputStream.close();

            fs.close();

        }

    }


}
