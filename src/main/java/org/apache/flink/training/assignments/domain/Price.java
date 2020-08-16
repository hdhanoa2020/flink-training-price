package org.apache.flink.training.assignments.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class Price extends IncomingEvent{
    private String id;
    private String cusip;
    private BigDecimal price;
    private long effectiveDateTime;
   // private long timestamp;
    private boolean eos;

    @Override
    public byte[] key() {
        return new byte[0];
    }
}

/*
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Objects;

public class Price implements Serializable {
    //private static final long serialVersionUID = 7946698732780254209L;
    private static final long serialVersionUID = -88984343343438484L;
    private String id;
    private String cusip;
    private BigDecimal price;
    private long effectiveDateTime;
    private long timestamp;

    public Price() {
    }

    public Price(String cusip,String id,long timestamp, long effectiveDateTime, BigDecimal price) {
        this.id = id;
        this.timestamp = timestamp;
        this.effectiveDateTime = effectiveDateTime;
        this.cusip = cusip;
        this.price = price;

    }

    public boolean isEos() {
        return eos;
    }

    public void setEos(boolean eos) {
        this.eos = eos;
    }

    private boolean eos;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Price)) return false;
        Price price1 = (Price) o;
        return getEffectiveDateTime() == price1.getEffectiveDateTime() &&
                getTimestamp() == price1.getTimestamp() &&
                Objects.equals(getId(), price1.getId()) &&
                Objects.equals(getCusip(), price1.getCusip()) &&
                Objects.equals(getPrice(), price1.getPrice());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getCusip(), getPrice(), getEffectiveDateTime(), getTimestamp());
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setCusip(String cusip) {
        this.cusip = cusip;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public void setEffectiveDateTime(long effectiveDateTime) {
        this.effectiveDateTime = effectiveDateTime;
    }


    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public String getCusip() {
        return cusip;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public long getEffectiveDateTime() {
        return effectiveDateTime;
    }


    public long getTimestamp() {
        return timestamp;
    }


}
*/