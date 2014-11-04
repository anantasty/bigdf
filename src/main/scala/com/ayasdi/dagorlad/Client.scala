//package com.ayasdi.dagorlad.api
//
//import java.io.File
//import java.util.concurrent.ExecutionException
//
//import com.ayasdi.dagorlad.anemone.Anemone
//import com.ayasdi.dagorlad.anemone.client.AnemoneClient
//import com.ayasdi.dagorlad.common.client.AppClient.Credentials
//import com.ayasdi.dagorlad.files.client.FilesClient
//import com.ayasdi.dagorlad.helloworld.HelloWorld
//import com.ayasdi.dagorlad.helloworld.client.HelloWorldClient
//import com.ayasdi.soil.utils.CryptoUtil
//
//class Client(user: String, password: String, host: String, port: Int) {
//    object Creds extends Credentials {
//        val userId = CryptoUtil.getUserId(user)
//        val key = CryptoUtil.getKeyFromPassphraseAES(password, userId)
//
//        override def getUserId = userId
//        override def getKey = key
//    }
//    def uploadFile(file: String) {
//        val files = new FilesClient(Creds, host, port)
//        val anemone = new AnemoneClient(Creds, host, port)
//        val helloworld = new HelloWorldClient(Creds, host, port)
//        try {
//            helloworld.callFn(HelloWorld.Param.Greet.newBuilder().setName(user).build()).get()
//        } catch {
//            case _: InterruptedException =>
//                println("Login interrupted'"); return ;
//            case e: ExecutionException =>
//                println("Invalid credentials or host/port: " + e.getCause().getMessage()); return ;
//        }
//
//        try {
//            val fileId = files.uploadFile(new File(file), null).get().getFileId();
//            val paramProto = Anemone.Param.CreateSourceFromFile.newBuilder().setFileId(fileId);
//            val sourceProto = anemone.callFn(paramProto).get().getCreateSourceFromFile().getSource();
//            println("Source created: " + sourceProto.getTableSummary().getName());
//        } catch {
//            case _: InterruptedException =>
//                println("Upload interrupted'"); return ;
//            case e: ExecutionException =>
//                println("Invalid credentials or host/port: " + e.getCause().getMessage()); return ;
//        }
//    }
//}