package io.wizzie.cep;

import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.cep.builder.Builder;
import io.wizzie.cep.logo.LogoPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cep {
    private static final Logger log = LoggerFactory.getLogger(Cep.class);

    public static void main(String args[]) throws Exception {
        LogoPrinter.PrintLogo();
        if (args.length == 1) {
            Config config = new Config(args[0]);
            Builder builder = new Builder(config.clone());

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    builder.close();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
                log.info("Stopped CEP process.");
            }));
        } else {
            log.error("Execute: java -cp ${JAR_PATH} io.wizzie.cep <config_file>");
        }
    }
}
