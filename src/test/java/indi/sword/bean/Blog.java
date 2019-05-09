package indi.sword.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author jeb_lin
 * 上午11:18 2019/5/9
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class Blog implements Serializable {
    private static final long serialVersionUID = 5231134212346077681L;

    private String User;
    private String date;
    private String message;
}
