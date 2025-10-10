<?php

declare(strict_types=1);

namespace Nosto\NostoIntegration\Model\Operation;

use Doctrine\DBAL\Connection;
use Nosto\NostoIntegration\Async\CategorySyncMessage;
use Nosto\NostoIntegration\Async\FullCatalogSyncMessage;
use Nosto\NostoIntegration\Async\ProductSyncMessage;
use Nosto\NostoIntegration\Model\ConfigProvider;
use Nosto\Scheduler\Model\Job\GeneratingHandlerInterface;
use Nosto\Scheduler\Model\Job\JobHandlerInterface;
use Nosto\Scheduler\Model\Job\JobResult;
use Nosto\Scheduler\Model\Job\Message\InfoMessage;
use Nosto\Scheduler\Model\JobScheduler;
use Shopware\Core\Framework\Context;
use Shopware\Core\Framework\DataAbstractionLayer\Dbal\Common\RepositoryIterator;
use Shopware\Core\Framework\DataAbstractionLayer\EntityRepository;
use Shopware\Core\Framework\DataAbstractionLayer\Search\Criteria;
use Shopware\Core\Framework\DataAbstractionLayer\Search\Filter\{EqualsFilter, NotFilter};
use Shopware\Core\Framework\Uuid\Uuid;

class FullCatalogSyncHandler implements JobHandlerInterface, GeneratingHandlerInterface
{
    public const HANDLER_CODE = 'nosto-integration-full-catalog-sync';

    private const BATCH_SIZE = 150;

    public function __construct(
        private readonly Connection $connection,
        private readonly EntityRepository $categoryRepository,
        private readonly JobScheduler $jobScheduler,
        private readonly ConfigProvider $configProvider,
    ) {
    }

    /**
     * @param FullCatalogSyncMessage $message
     */
    public function execute(object $message): JobResult
    {
        $size = $this->configProvider->getBatchSize();
        $size = ($size < 1) ? self::BATCH_SIZE : $size;

        $result = new JobResult();
        $result->addMessage(new InfoMessage('Child job generation started.'));

        foreach ($this->fetchParentProducts($size, $message->getContext()) as $productIds) {
            $this->jobScheduler->schedule(
                new ProductSyncMessage(
                    Uuid::randomHex(),
                    $message->getJobId(),
                    $productIds,
                    $message->getContext(),
                ),
            );
            $result->addMessage(
                new InfoMessage('Job with payload of: ' . count($productIds) . ' products has been scheduled.'),
            );
        }

        $criteriaCategory = new Criteria();
        $criteriaCategory->addFilter(new NotFilter(NotFilter::CONNECTION_AND, [
            new EqualsFilter('parentId', null),
        ]));
        $criteriaCategory->setLimit(self::BATCH_SIZE);
        $categoryRepositoryIterator = new RepositoryIterator(
            $this->categoryRepository,
            $message->getContext(),
            $criteriaCategory,
        );
        while (($categoryIds = $categoryRepositoryIterator->fetchIds()) !== null) {
            $ids = array_combine($categoryIds, $categoryIds);
            $this->jobScheduler->schedule(
                new CategorySyncMessage(
                    Uuid::randomHex(),
                    $message->getJobId(),
                    $ids,
                    $message->getContext(),
                ),
            );
            $result->addMessage(
                new InfoMessage('Job with payload of: ' . count($ids) . ' categories has been scheduled.'),
            );
        }

        return $result;
    }

    protected function fetchParentProducts(int $batchSize, Context $context): iterable
    {
        $query = $this->connection->createQueryBuilder()
            ->select(
                'LOWER(HEX(p.id)) AS id',
                'p.product_number AS productNumber',
            )
            ->from('product', 'p')
            ->where('p.parent_id IS NULL')
            ->andWhere('p.version_id = :version_id')
            ->setParameter('version_id', Uuid::fromHexToBytes($context->getVersionId()))
            ->setMaxResults($batchSize);

        $offset = 0;
        do {
            $query->setFirstResult($offset);

            $queryResult = $query->executeQuery();
            $offset += $queryResult->rowCount();

            $result = [];
            foreach ($queryResult->fetchAllAssociative() as $row) {
                $id = $row['id'] ?? null;
                $productNumber = $row['productNumber'] ?? null;
                if (!is_string($id) || !is_string($productNumber)) {
                    continue;
                }

                $result[$id] = $productNumber;
            }

            if (!empty($result)) {
                yield $result;
            }
        } while ($queryResult->rowCount() > 0);
    }
}
