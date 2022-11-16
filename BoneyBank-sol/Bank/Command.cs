using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Bank {
    public class Command {

        public enum RequestType {
            ReadBalance,
            Deposit,
            Withdrawal,
            Unknown
        }

        private Tuple<long, long> id;
        private RequestType type;
        private float value;

        public Command(Tuple<long, long> id, RequestType type, float value) {
            this.id = id;
            this.type = type;
            this.value = value;
        }

        public Command(Tuple<long, long> id) {
            this.id = id;
            this.type = RequestType.Unknown;
            this.value = -2;
        }

        public Tuple<long, long> getId() {
            return id;
        }

        public RequestType getType() {
            return type;
        }

        public float getValue() {
            return value;
        }

        public override string ToString() {
            return "{ID:" + id + ", Type: " + type + ", Value: " + value + "}";
        }
    }
}
