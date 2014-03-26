package ru.mvideo.common.mapping.json;

import com.sap.aii.af.service.auditlog.Audit;
import com.sap.aii.mapping.api.AbstractTransformation;
import com.sap.aii.mapping.api.StreamTransformationException;
import com.sap.aii.mapping.api.TransformationInput;
import com.sap.aii.mapping.api.TransformationOutput;
import com.sap.engine.interfaces.messaging.api.MessageDirection;
import com.sap.engine.interfaces.messaging.api.MessageKey;
import com.sap.engine.interfaces.messaging.api.auditlog.AuditLogStatus;
import org.json.JSONObject;
import org.json.XML;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by rassakhatsky on 19.03.14.
 */
public class IA_JSON_2_XML extends AbstractTransformation {
    private static final String DASH = "-";
    private String msgID;                       //Message ID
    private String uuidTimeLow;                 //The low field of the timestamp (0-3)
    private String uuidTimeMid;                 //The middle field of the timestamp (4-5)
    private String uuidTimeHighAndVersion;      //The high field of the timestamp multiplexed with the version number (6-7)
    private String uuidClockSeqAndReserved;     //The high field of the clock sequence multiplexed with the variant (8)
    private String uuidClockSeqLow;             //The low field of the clock sequence (9)
    private String uuidNode;                    //The spatially unique node identifier (10-15)
    private String msgUUID;                     //Message UUID (Universally unique identifier)
    private MessageKey msgKey;                  //Message Key (SAP Message ID)
    private boolean logLevel = true;
    private String rootTag = "message";

    @Override
    public void transform(TransformationInput transformationInput, TransformationOutput transformationOutput)
            throws StreamTransformationException {
        try {
            //Get Message ID (For Audit Log)
            String msgID = transformationInput.getInputHeader().getMessageId();
            uuidTimeLow = msgID.substring(0, 8);
            uuidTimeMid = msgID.substring(8, 12);
            uuidTimeHighAndVersion = msgID.substring(12, 16);
            uuidClockSeqAndReserved = msgID.substring(16, 18);
            uuidClockSeqLow = msgID.substring(18, 20);
            uuidNode = msgID.substring(20, 32);
            msgUUID = uuidTimeLow + DASH + uuidTimeMid
                    + DASH + uuidTimeHighAndVersion
                    + DASH + uuidClockSeqAndReserved + uuidClockSeqLow
                    + DASH + uuidNode;
            msgKey = new MessageKey(msgUUID, MessageDirection.OUTBOUND);


            //Mapping transformation have been started
            if (logLevel)
                Audit.addAuditLogEntry(msgKey, AuditLogStatus.SUCCESS, "JAVA transformation from JSON to XML was started+\\n"
                        + "Message ID - " + msgID);

            //Payload
            InputStream is = transformationInput.getInputPayload().getInputStream();
            String jsonString = readStream(is);
            JSONObject jSONObject = new JSONObject(jsonString);
            String xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?>" + "<message>" + XML.toString(jSONObject) + "</message>";
            transformationOutput.getOutputPayload().getOutputStream().write(xml.toString().getBytes("UTF-8"));
            if (logLevel)
                Audit.addAuditLogEntry(msgKey, AuditLogStatus.SUCCESS, "JAVA transformation from JSON to XML has been completed");
        } catch (Exception exception) {
            if (logLevel) Audit.addAuditLogEntry(msgKey, AuditLogStatus.ERROR, "Error in transformation");
            throw new StreamTransformationException(exception.toString());
        }
    }

    private String readStream(InputStream in) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        StringBuilder out = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            out.append(line);
        }
        return out.toString();
    }
}
