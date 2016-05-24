package com.xjeffrose.chicago.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.xjeffrose.chicago.ChiConfig;
import com.xjeffrose.chicago.client.ChicagoClient;
import com.xjeffrose.xio.processor.XioProcessor;
import com.xjeffrose.xio.server.RequestContext;
import com.xjeffrose.xio.server.Route;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.json.JsonObjectDecoder;
import io.netty.util.CharsetUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpMethod.*;

/**
 * Created by root on 5/24/16.
 */
public class ChicagoRestProcessor implements XioProcessor{
    private static final Logger log = Logger.getLogger(ChicagoRestProcessor.class.getName());
    private final ChiConfig config;

    public ChicagoRestProcessor( ChiConfig config ){
        this.config = config;
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx) {
        // what does this override?
    }

    @Override
    public ListenableFuture<Boolean> process(ChannelHandlerContext ctx, Object req, RequestContext reqCtx) {
        ListeningScheduledExecutorService executor = MoreExecutors.listeningDecorator(ctx.executor());
        ListenableFuture<Boolean> responseFuture = executor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                FullHttpRequest httpRequest = null;
                Request request = null;
                HttpResponse response = null;
                Gson gson = new Gson();

                if (req instanceof FullHttpRequest) {
                    httpRequest = (FullHttpRequest) req;
                }

                log.debug(httpRequest);
                httpRequest.headers().forEach(xs -> log.debug(xs.getKey() + ": " + xs.getValue()));
                QueryStringDecoder queryStringDecoder = new QueryStringDecoder(httpRequest.uri());
                Map<String, List<String>> params = queryStringDecoder.parameters();
                if(httpRequest.headers().contains(HttpHeaderNames.CONTENT_TYPE) && httpRequest.headers().get(HttpHeaderNames.CONTENT_TYPE.toString()).contains("json")){
                    request = gson.fromJson(httpRequest.content().toString(CharsetUtil.UTF_8),Request.class);
                }

                Route client = Route.build("/rest/v1/");
                Map<Route, Method> routeMap = new HashMap<>();
                String query = queryStringDecoder.path();
                if (query != null && client.matches(query)) {
                    response = processRequest(ctx,httpRequest.method(),request,params);
                } else {
                    response = createBadResponse(ctx);
                    reqCtx.setContextData(reqCtx.getConnectionId(), response);
                    return true;
                }

                reqCtx.setContextData(reqCtx.getConnectionId(), response);
                return true;
            }
        });
        return responseFuture;
    }

    public HttpResponse processRequest(ChannelHandlerContext ctx,HttpMethod method, Request request, Map<String, List<String>> params){
        Response response = new Response();
        String json = null;
        try {
            if (method.equals(HttpMethod.POST)) {
                ChicagoClient chicagoClient = new ChicagoClient(config.getZkHosts());
                json = String.valueOf(chicagoClient.write(request.getKey().getBytes(), request.getValue().getBytes()));
            } else if (method.equals(HttpMethod.GET)) {
                ChicagoClient chicagoClient = new ChicagoClient(config.getZkHosts());
                json = new String(chicagoClient.read(params.get("key").get(0).getBytes()));
            } else if (method.equals(HttpMethod.DELETE)) {
                json = "true";
            } else {
                json = "Bad request";
            }
        }catch (Exception e){
            log.info(e.getStackTrace());
            json = "Bad request";
        }
        response.setValue(json);
        return createResponse(ctx,response);
    }

    public HttpResponse createResponse(ChannelHandlerContext ctx, Response response){
        StringWriter sw = new StringWriter();
        final ObjectMapper mapper = new ObjectMapper();
        try {
            mapper.writeValue(sw, response);
        } catch(IOException e) {
            log.error(e.getMessage());
        }
        String json = sw.toString();
        DefaultFullHttpResponse httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, ctx.alloc().buffer().writeBytes(json.getBytes()));
        httpResponse.headers().set(CONTENT_TYPE, "Application/json");
        httpResponse.headers().set(CONTENT_LENGTH, httpResponse.content().readableBytes());
        return httpResponse;
    }

    public HttpResponse createBadResponse(ChannelHandlerContext ctx){
        Response res = new Response();
        res.setValue("Bad request");
        return createResponse(ctx,res);
    }
}
