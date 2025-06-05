package dal

type ITestDataDao interface {
	GetRandomPdu(maxOperations int) string
}
