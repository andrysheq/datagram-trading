module parallelprgrm.cw.datagramtrading {
    requires javafx.controls;
    requires javafx.fxml;

    requires org.kordamp.bootstrapfx.core;

    opens parallelprgrm.cw.datagramtrading to javafx.fxml;
    exports parallelprgrm.cw.datagramtrading;
}