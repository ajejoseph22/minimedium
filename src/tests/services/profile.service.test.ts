import prisma from '../../prisma/prisma-client';
import { followUser, getProfile, unfollowUser } from '../../app/routes/profile/profile.service';

jest.mock('../../prisma/prisma-client', () => ({
  __esModule: true,
  default: {
    user: {
      findUnique: jest.fn(),
      update: jest.fn(),
    },
  },
}));

describe('ProfileService', () => {
  const createdAt = new Date('2026-02-01T00:00:00.000Z');
  const updatedAt = new Date('2026-02-02T00:00:00.000Z');
  const prismaMock = prisma as unknown as {
    user: {
      findUnique: jest.Mock;
      update: jest.Mock;
    };
  };

  const createMockUser = (overrides = {}) => ({
    id: 123,
    username: 'RealWorld',
    email: 'realworld@me',
    name: null,
    role: 'user',
    active: true,
    password: '1234',
    bio: null,
    image: null,
    demo: false,
    createdAt,
    updatedAt,
    followedBy: [],
    ...overrides,
  });

  beforeEach(() => {
    prismaMock.user.findUnique.mockReset();
    prismaMock.user.update.mockReset();
  });

  describe('getProfile', () => {
    test('should return a following property', async () => {
      // Given
      const username = 'RealWorld';
      const id = 123;

      const mockedResponse = createMockUser();

      // When
      prismaMock.user.findUnique.mockResolvedValue(mockedResponse);

      // Then
      await expect(getProfile(username, id)).resolves.toHaveProperty('following');
    });

    test('should throw an error if no user is found', async () => {
      // Given
      const username = 'RealWorld';
      const id = 123;

      // When
      prismaMock.user.findUnique.mockResolvedValue(null);

      // Then
      await expect(getProfile(username, id)).rejects.toThrowError();
    });
  });

  describe('followUser', () => {
    test('should return a following property', async () => {
      // Given
      const usernamePayload = 'AnotherUser';
      const id = 123;

      const mockedAuthUser = createMockUser();
      const mockedResponse = createMockUser({
        username: 'AnotherUser',
        email: 'another@me',
      });

      // When
      prismaMock.user.findUnique.mockResolvedValue(mockedAuthUser);
      prismaMock.user.update.mockResolvedValue(mockedResponse);

      // Then
      await expect(followUser(usernamePayload, id)).resolves.toHaveProperty('following');
    });

    test('should throw an error if no user is found', async () => {
      // Given
      const usernamePayload = 'AnotherUser';
      const id = 123;

      // When
      prismaMock.user.findUnique.mockResolvedValue(null);

      // Then
      await expect(followUser(usernamePayload, id)).rejects.toThrowError();
    });
  });

  describe('unfollowUser', () => {
    test('should return a following property', async () => {
      // Given
      const usernamePayload = 'AnotherUser';
      const id = 123;

      const mockedAuthUser = createMockUser();
      const mockedResponse = createMockUser({
        username: 'AnotherUser',
        email: 'another@me',
      });

      // When
      prismaMock.user.findUnique.mockResolvedValue(mockedAuthUser);
      prismaMock.user.update.mockResolvedValue(mockedResponse);

      // Then
      await expect(unfollowUser(usernamePayload, id)).resolves.toHaveProperty('following');
    });

    test('should throw an error if no user is found', async () => {
      // Given
      const usernamePayload = 'AnotherUser';
      const id = 123;

      // When
      prismaMock.user.findUnique.mockResolvedValue(null);

      // Then
      await expect(unfollowUser(usernamePayload, id)).rejects.toThrowError();
    });
  });
});
