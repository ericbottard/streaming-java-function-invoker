package io.projectriff.invoker;

import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.ObjectToStringHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

import java.util.List;

public class HttpMessageUtils {

    public static final String RIFF_INPUT = "RiffInput";
    public static final String RIFF_OUTPUT = "RiffOutput";
    public static final String CONTENT_TYPE = "ContentType";

    private HttpMessageUtils() {

    }

    public static void installDefaultConverters(List<HttpMessageConverter> converters) {
        converters.clear();
        converters.add(new MappingJackson2HttpMessageConverter());
        converters.add(new FormHttpMessageConverter());
        StringHttpMessageConverter sc = new StringHttpMessageConverter();
        sc.setWriteAcceptCharset(false);
        converters.add(sc);
        ObjectToStringHttpMessageConverter oc = new ObjectToStringHttpMessageConverter(new DefaultConversionService());
        oc.setWriteAcceptCharset(false);
        converters.add(oc);
    }
}
