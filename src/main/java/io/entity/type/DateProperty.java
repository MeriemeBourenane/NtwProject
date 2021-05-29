package io.entity.type;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateProperty extends Property<Date> {
    @Override
    public void setValue(String input) {
        try {
            this.value = new SimpleDateFormat("YYYY-MM-dd HH:mm:SS").parse(input);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
