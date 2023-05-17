package deliveryapp.domain;

import deliveryapp.OverseasDeliveryApplication;
import deliveryapp.domain.OverseasDeliveryStarted;
import java.util.Date;
import java.util.List;
import javax.persistence.*;
import lombok.Data;

@Entity
@Table(name = "OverseasDelivery_table")
@Data
public class OverseasDelivery {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private String address;

    private String countryCode;

    private String userId;

    private String qty;

    private Long orderId;

    @PostPersist
    public void onPostPersist() {
        OverseasDeliveryStarted overseasDeliveryStarted = new OverseasDeliveryStarted(
            this
        );
        overseasDeliveryStarted.publishAfterCommit();
    }

    public static OverseasDeliveryRepository repository() {
        OverseasDeliveryRepository overseasDeliveryRepository = OverseasDeliveryApplication.applicationContext.getBean(
            OverseasDeliveryRepository.class
        );
        return overseasDeliveryRepository;
    }
}
