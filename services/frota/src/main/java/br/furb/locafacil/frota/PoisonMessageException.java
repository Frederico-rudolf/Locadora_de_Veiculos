package br.furb.locafacil.frota;

/** Mensagem inválida: não é apagada; o SQS reentrega até maxReceiveCount e então manda para a DLQ (F4). */
public class PoisonMessageException extends RuntimeException {
    public PoisonMessageException(String msg) { super(msg); }
}
