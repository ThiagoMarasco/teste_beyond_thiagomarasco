import logging
import sys

class SafeFormatter(logging.Formatter):
    def format(self, record):
        record.linha = getattr(record, "linha", "")
        record.campo = getattr(record, "campo", "")
        record.valor = getattr(record, "valor", "")
        record.motivo = getattr(record, "motivo", "")
        return super().format(record)

logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)

formatter = SafeFormatter(
    "%(asctime)s - %(message)s %(linha)s - %(campo)s - %(valor)s - %(motivo)s",
    "%Y-%m-%d %H:%M:%S"
)

# Handler para arquivo
file_handler = logging.FileHandler("output/erros_validacao.log", encoding="utf-8")
file_handler.setFormatter(formatter)

# Handler para console (VSCode Debug)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)